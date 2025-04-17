import time
import asyncio
import aiohttp

from asyncio import Queue
from aiohttp import ClientSession
from typing import Optional, List, Set
from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin
from concurrent.futures import ProcessPoolExecutor

MAX_TIME = 20
MAX_TASKS = 200
MAX_PROCESSES = 8
START_URL = "https://www.wikipedia.org/"


# CPU-bound tasks should be run in a process pool
def scrape_links(page: str, link: str) -> Optional[List[str]]:
    try:
        soup = BeautifulSoup(page, "html.parser")
        return [
            urljoin(link, str(href))
            for a in soup.find_all("a", href=True)
            if isinstance(a, Tag) and (href := a.get("href")) is not None
        ]
    except:
        return None


# I/O-bound tasks should be run in an event loop
async def scrape_page(session: ClientSession, link: str) -> Optional[str]:
    try:
        async with session.get(
            link, timeout=aiohttp.ClientTimeout(total=1)
        ) as response:
            if "text/html" not in response.headers.get("Content-Type", ""):
                return None

            return await response.text()
    except:
        return None


async def task(
    start_time: float,
    links: Set[str],
    queue: Queue[str],
    session: ClientSession,
    executor: ProcessPoolExecutor,
) -> None:
    while (time.time() - start_time) < MAX_TIME:
        try:
            link = await asyncio.wait_for(queue.get(), timeout=1)

            page = await scrape_page(session, link)

            if not page:
                continue

            loop = asyncio.get_running_loop()
            scraped_links = await loop.run_in_executor(
                executor, scrape_links, page, link
            )

            if not scraped_links:
                continue

            for scraped_link in scraped_links:
                if scraped_link not in links:
                    links.add(scraped_link)
                    await queue.put(scraped_link)

        finally:
            queue.task_done()


async def main() -> None:
    links = set()
    queue = asyncio.Queue()
    await queue.put(START_URL)

    start_time = time.time()
    async with aiohttp.ClientSession() as session:
        with ProcessPoolExecutor(max_workers=MAX_PROCESSES) as executor:
            tasks = [
                asyncio.create_task(task(start_time, links, queue, session, executor))
                for _ in range(MAX_TASKS)
            ]
            await asyncio.gather(*tasks)
    end_time = time.time()

    print(f"Scraped {len(links)} unique links in {end_time - start_time}s")


if __name__ == "__main__":
    asyncio.run(main())
