import time
import uuid
from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer


def test_failed_registration(account: AccountApi, mail: MailApi) -> None:
    expected_mail = "string@mail.ru"
    account.register_user(login="string", email=expected_mail, password="string")
    for _ in range(10):
        response = mail.find_message(query=expected_mail)
        if response.json()["total"] > 0:
            raise AssertionError(f"Письмо для адреса {expected_mail} найдено")
        time.sleep(1)


def test_success_registration(account: AccountApi, mail: MailApi) -> None:
    base = uuid.uuid4().hex
    expected_mail = f"{base}_api@yandex.ru"
    account.register_user(login=base, email=expected_mail, password="123456")
    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError(f"Письмо для пользователя {base} не найдено")


def tests_success_registration_with_kafka_producer(mail: MailApi, kafka_producer: Producer) -> None:
    base = uuid.uuid4().hex
    messages = {
        "login": base,
        "email": f"{base}_producer@yandex.ru",
        "password": "123456"
    }
    kafka_producer.send("register-events", messages)
    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError(f"Письмо для пользователя {base} не найдено")


def test_register_events_error_consumer(account: AccountApi, mail: MailApi, kafka_producer: Producer) -> None:
    login = uuid.uuid4().hex
    email = f"{login}_consumer@yandex.ru"
    message = {
        "input_data": {
            "login": login,
            "email": email,
            "password": "123456"
        },
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
