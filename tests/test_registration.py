import time
import uuid
from framework.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from framework.helpers.kafka.consumers.register_events_errors import RegisterEventsErrorsSubscriber
from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer
from framework.internal.rmq.publisher import RmqPublisher


def test_failed_registration(
        account: AccountApi,
        mail: MailApi
) -> None:
    expected_mail = "string@mail.ru"
    account.register_user(login="string", email=expected_mail, password="string")
    for _ in range(10):
        response = mail.find_message(query=expected_mail)
        if response.json()["total"] > 0:
            raise AssertionError(f"Письмо для адреса {expected_mail} найдено")
        time.sleep(1)


def test_success_registration(
        register_events_subscriber: RegisterEventsSubscriber,
        register_message: dict[str, str],
        account: AccountApi,
        mail: MailApi
) -> None:
    login = register_message["login"]
    account.register_user(**register_message)
    register_events_subscriber.find_message(login=login)
    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError(f"Письмо для пользователя {login} не найдено")


def tests_success_registration_with_kafka_producer(
        register_message: dict[str, str],
        mail: MailApi,
        kafka_producer: Producer
) -> None:
    login = register_message["login"]
    kafka_producer.send("register-events", register_message)
    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError(f"Письмо для пользователя {login} не найдено")


def test_register_events_error_consumer(
        register_message: dict[str, str],
        account: AccountApi,
        mail: MailApi,
        kafka_producer: Producer
) -> None:
    login = register_message["login"]
    email = register_message["email"]
    message = {
        "input_data": register_message,
        "error_message": {
            "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
            "title": "Validation failed",
            "status": 400,
            "traceId": "00-2bd2ede7c3e4dcf40c4b7a62ac23f448-839ff284720ea656-01",
            "errors": {
                "Email": ["Invalid"]
            }
        },
        "error_type": "unknown"
    }
    kafka_producer.send("register-events-errors", message)
    for _ in range(10):
        response = mail.find_message(query=email)
        response_data = response.json()
        if response_data.get("total", 0) > 0:
            try:
                token = response_data["items"][0]["Content"]["Body"].split("/")[-1].rstrip('"}')
                break
            except (KeyError, IndexError, ValueError, TypeError, AttributeError):
                raise AssertionError(f"Токен для пользователя {login} не был получен")
        time.sleep(1)
    else:
        raise AssertionError(f"Письмо для адреса {email} не найдено")
    response = account.activate_user(token=token)
    assert response.status_code == 200, f"Ожидался статус 200, получен {response.status_code}"


def tests_success_registration_with_kafka_producer_consumer(
        register_message: dict[str, str],
        register_events_subscriber: RegisterEventsSubscriber,
        kafka_producer: Producer,
) -> None:
    login = register_message["login"]
    kafka_producer.send("register-events", register_message)
    register_events_subscriber.find_message(login=login)


def test_failed_registration_with_error_type_validation(
        register_message: dict[str, str],
        account: AccountApi,
        register_events_subscriber: RegisterEventsSubscriber,
        register_events_errors_subscriber: RegisterEventsErrorsSubscriber,
) -> None:
    login = register_message["login"]
    account.register_user(login=login, email=login, password=register_message["password"])
    register_events_subscriber.find_message(login=login)
    register_events_errors_subscriber.find_message(login=login, error_type="validation")


def test_register_events_errors_with_error_type_unknown(
        register_events_errors_subscriber: RegisterEventsErrorsSubscriber,
        kafka_producer: Producer,
) -> None:
    message = {
        "input_data": {
            "login": "string",
            "email": "string@mail.ru",
            "password": "string"
        },
        "error_message": {
            "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
            "title": "Validation failed",
            "status": 400,
            "traceId": "00-d957971fc5a90855accb02b59e879f3c-fa1d384d99ca048d-01",
            "errors": {
                "Email": ["Taken"]
            }
        },
        "error_type": "unknown"
    }
    kafka_producer.send("register-events-errors", message)
    register_events_errors_subscriber.find_message(login="string", error_type="validation")


def test_rmq(
        rmq_publisher: RmqPublisher,
        mail: MailApi
) -> None:
    address = f"{uuid.uuid4().hex}@mail.ru"
    message = {
        "address": address,
        "subject": " My publisher message",
        "body": "My publisher message",
    }
    rmq_publisher.publisher("dm.mail.sending", message)
    for _ in range(10):
        response = mail.find_message(query=address)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError(f"Письмо для адреса {address} не найдено")
