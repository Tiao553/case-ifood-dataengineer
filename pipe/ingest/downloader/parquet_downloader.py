import requests
from tqdm import tqdm
from utils.logger_config import setup_logger

logger = setup_logger()

def download_to_local(url: str, local_path: str) -> None:
    try:
        headers =  {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/octet-stream",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Referer": "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
        }
        response = requests.get(url, headers=headers, stream=True, timeout=60)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        with open(local_path, "wb") as file, tqdm(
            desc=local_path,
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
                    bar.update(len(chunk))

        logger.info(f"Download concluído: {local_path}")

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"Erro HTTP em {url}: {http_err}")
    except Exception as err:
        logger.error(f"Erro geral em {url}: {err}")
