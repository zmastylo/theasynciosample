import asyncio

from asyncdatahandler import AsyncDataHandler
from asyncextractor import AsyncExtractor

if __name__ == "__main__":
    loop = None
    try:
        loop = asyncio.get_event_loop()
        urls = [
            'http://headers.jsontest.com/',
            'http://ip.jsontest.com/'
        ]

        extractor = AsyncExtractor(loop, urls=urls)
        handler = AsyncDataHandler(extractor)
        loop.run_until_complete(handler.handle())
    except Exception as e:
        print('ERROR: {}'.format(str(e)))
    finally:
        if not loop:
            loop.close()
