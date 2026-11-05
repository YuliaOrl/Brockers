import time
from framework.internal.kafka.subscriber import Subscriber


class RegisterEventsErrorsSubscriber(Subscriber):
    topic: str = "register-events-errors"

    def find_message(self, login: str, error_type: str, timeout: float = 20.0) -> None:
        start_time = time.time()
        while time.time() - start_time < timeout:
            message = self.get_message(timeout=timeout)
            if message.value["input_data"]["login"] == login and message.value["error_type"] == error_type:
                break
        else:
            raise AssertionError(
                f"Сообщение в топике {self.topic} для пользователя {login} с типом ошибки {error_type} не найдено"
                )
