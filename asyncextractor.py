import asyncio
import concurrent

import requests
from aiohttp import ClientSession


class AsyncExtractor:
    __RATE_LIMIT_REACHED = 429

    def __init__(self, event_loop, urls, max_workers=10):
        self.loop = event_loop
        self.max_workers = max_workers
        self.urls = urls

    async def extract(self):
        return await self._gather()

    async def _gather(self):
        """Go over list of uris (urls) and process get requests
        asynchronously. That of course, can be done for just one url
        :return: futures"""
        try:
            futures = [
                self.loop.run_in_executor(None, self._get, url)
                for url in self.urls
            ]
            return futures
        except Exception as e:
            print('AsyncExtractor.__gather - error: shit blew up' + str(e))
            raise e

    def _get(self, url):
        """A regular GET, wrapped for convenience
        :param url:
        :return: response"""
        response = requests.get(url=url)
        if response.status_code == self.__RATE_LIMIT_REACHED:
            raise Exception('Rate limit exceeded - need to wait 1 hour')
        return response

    # The code in below shows other ways of doing what we do above
    # I like the above
    async def _gather_tpe(self):
        """You can employ a specific executor as in here we use
        concurrent.futures.ThreadPoolExecutor. No need really, what you see
        in _gather() is good.
        :return: futures"""
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as tpe:
                futures = [
                    self.loop.run_in_executor(tpe, requests.get, url)
                    for url in self.urls
                ]
            return futures
        except Exception as e:
            print('AsyncExtractor.__gather - error: shit blew up' + str(e))
            raise e

    async def _gather_with_session(self):
        """
        Another way of sending a get with aiohttp session
        I didn't see much difference in performance doing this
        vs normal wrapped get (as in __get)
        :return:
        """
        async with ClientSession() as session:
            tasks = []
            for url in self.urls:
                tasks.append(self._get(session, url=url))

            result = await asyncio.gather(*tasks, return_exceptions=True)
            return result

    async def _get_with_session(self, session: ClientSession, url, **kwargs):
        """
        Another way to do async GET via ClientSession. I didn't see much difference
        vs requests.get plugged into execution loop:
        elf.loop.run_in_executor(tpe, requests.get, uri
        :param session: client session
        :param url:
        :param kwargs:
        :return: data retrieved via get
        """
        response = await session.request('GET', url=url, **kwargs)

        # Example status showing rate limit reach on a certain API
        # not like we will have it, but for illustration purposes
        if response.status == self.__RATE_LIMIT_REACHED:
            raise Exception('Rate limit exceeded - need to wait 1 hour')

        # Assuming you expect JSON
        data = await response.json()
        return data
