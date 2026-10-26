import httpx


class MailApi:
    def __init__(self, base_url: str = "http://185.185.143.231:8085") -> None:
        # http://185.185.143.231:8085/mail/mail/search?limit=50&kind=containing&start=0
        self._base_url = base_url
        self._client = httpx.Client(base_url=self._base_url)

    def find_message(self, query: str) -> httpx.Response:
        param = {
            "query": query,
            "limit": 1,
            "kind": "containing",
            "start": 0
        }
        response = self._client.get(url="/mail/mail/search", params=param)
        print(response.content)
        return response
