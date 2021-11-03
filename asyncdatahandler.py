import asyncio
import json


class AsyncDataHandler:
    """
    Asyn data handler: assumes JSON format
    """
    def __init__(self, data_extractor):
        self.data_extractor = data_extractor

    async def handle(self):
        futures = await self.data_extractor.extract()
        for response in await asyncio.gather(*futures):
            json_data = json.loads(response.content.decode('utf-8'))
            print(json.dumps(json_data))

