import httpx


class AccountApi:
    def __init__(self, base_url: str = "http://185.185.143.231:8085") -> None:
        # http://185.185.143.231:8085/register/user/async-register
        self._base_url = base_url
        self._client = httpx.Client(base_url=self._base_url)

    def register_user(self, login: str, email: str, password: str) -> httpx.Response:
        data = {
            "login": login,
            "email": email,
            "password": password
        }
        response = self._client.post(url="/register/user/async-register", json=data)
        print(response.content)
        return response
