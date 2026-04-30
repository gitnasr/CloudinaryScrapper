import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import cloudinary
import cloudinary.api
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from tinydb import Query, TinyDB
from urllib3.util.retry import Retry

load_dotenv(".env")


class CScrapper:
    def __init__(self) -> None:
        self.cloudName = os.getenv("CLOUD_NAME")
        self.scheduler = BlockingScheduler()
        self.download_workers = max(1, int(os.getenv("DOWNLOAD_WORKERS", "8")))
        self.chunk_size = int(os.getenv("DOWNLOAD_CHUNK_SIZE", str(64 * 1024)))
        self.timeout = int(os.getenv("DOWNLOAD_TIMEOUT", "30"))
        self._thread_local = threading.local()
        try:
            self.cloud = cloudinary.config(
                cloud_name=self.cloudName,
                api_key=os.getenv("API_KEY"),
                api_secret=os.getenv("API_SECRET"),
            )
            self.all_resources = []
            self.next_run = {
                "last_cursor": None,
                "run_at": None,
                "page": 1
            }
            self.db = TinyDB("db.json")
        except KeyError as e:
            raise ValueError("Missing environment variable: {}".format(e))

    def _create_session(self):
        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        adapter = HTTPAdapter(
            max_retries=retries,
            pool_connections=self.download_workers,
            pool_maxsize=self.download_workers,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _get_session(self):
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = self._create_session()
            self._thread_local.session = session
        return session

    def extract_datetime(self, error_message):
        match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) UTC", error_message)
        if match:
            datetime_str = match.group(1)
            execution_time = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            return execution_time
        return None

    def _sanitize_relative_folder(self, folder):
        normalized_folder = (folder or "").replace("\\", "/").strip("/")
        if not normalized_folder:
            return ""

        safe_parts = []
        for part in normalized_folder.split("/"):
            if not part or part == ".":
                continue
            if part == "..":
                raise ValueError(f"Invalid folder path: {folder}")
            safe_parts.append(part)
        return os.path.join(*safe_parts) if safe_parts else ""

    def _resource_folder(self, resource):
        resource_folder = resource.get("folder") or os.path.dirname(resource.get("public_id", ""))
        return self._sanitize_relative_folder(resource_folder)

    def _resource_extension(self, resource):
        extension = resource.get("format")
        if extension:
            return extension
        return resource.get("resource_type", "bin")

    def _resource_file_path(self, resource, download_path):
        folder = self._resource_folder(resource)
        file_name = f"{resource['asset_id']}.{self._resource_extension(resource)}"
        if folder:
            return os.path.join(download_path, folder, file_name), file_name
        return os.path.join(download_path, file_name), file_name

    def schedule_next_run(self):
        if self.next_run["run_at"]:
            print(f"⏳ Rate limit exceeded. Retrying at {self.next_run['run_at']}...")
            self.scheduler.add_job(self.get_resources, 'date', run_date=self.next_run["run_at"])

    def get_resources(self, cursor=None, page=1):
        print("🔄 Fetching resources...")
        max_results = 50
        cursor = self.next_run["last_cursor"] or cursor
        page = self.next_run["page"] or page

        while True:
            try:
                resources = cloudinary.api.resources(max_results=max_results, next_cursor=cursor)
            except Exception as e:
                if "Rate Limit Exceeded" in str(e):
                    execution_time = self.extract_datetime(str(e))
                    if execution_time:
                        self.next_run["run_at"] = execution_time
                        self.next_run["last_cursor"] = cursor
                        self.next_run["page"] = page
                        self.schedule_next_run()
                    return  

            self.all_resources.extend(resources["resources"])
            cursor = resources.get("next_cursor")
            print(f"✅ Page {page} - {len(self.all_resources)} resources fetched")
            page += 1
            try:
                resources_table = self.db.table('resources')
                pagination_table = self.db.table('pagination')
               
               # Store resources with timestamp
                resources_table.insert_multiple([
                    {**resource, 'fetched_at': datetime.now().isoformat()}
                    for resource in resources["resources"]
                ])
                
                # Update pagination state
                pagination_table.upsert(
                    {'cursor': cursor, 'page': page, 'updated_at': datetime.now().isoformat()},
                    Query().page.exists()  # Update existing record
                )
            except Exception as e:
                print(f"❌ Failed to store data: {e}")
            if not cursor:
                break

        self.download_resource()
        self.all_resources = []
        self.next_run = {
            "last_cursor": None,
            "run_at": None,
            "page": 1
        }

    def _download_resource_file(self, resource, download_path):
        file_path, file_name = self._resource_file_path(resource, download_path)
        expected_size = resource.get("bytes")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        if os.path.exists(file_path) and expected_size and os.path.getsize(file_path) == expected_size:
            return "skipped", file_name, file_path

        session = self._get_session()
        response = None
        try:
            response = session.get(resource["secure_url"], stream=True, timeout=self.timeout)
            response.raise_for_status()

            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(self.chunk_size):
                    if chunk:
                        f.write(chunk)

            return "downloaded", file_name, file_path
        except requests.RequestException as e:
            return "failed", file_name, f"Network error: {e}"
        except IOError as e:
            return "failed", file_name, f"File error: {e}"
        except Exception as e:
            return "failed", file_name, f"{type(e).__name__}: {e}"
        finally:
            if response is not None:
                response.close()

    def download_resource(self):
        print("📥 Downloading resources...")
        download_path = os.path.join("downloads", self.cloudName)
        os.makedirs(download_path, exist_ok=True)
        total_resources = len(self.all_resources)
        if total_resources == 0:
            print("ℹ️ No resources to download.")
            return

        print(f"🧵 Starting {self.download_workers} download workers for {total_resources} resources")

        downloaded = 0
        skipped = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=self.download_workers) as executor:
            futures = [
                executor.submit(self._download_resource_file, resource, download_path)
                for resource in self.all_resources
            ]

            for future in as_completed(futures):
                status, file_name, details = future.result()
                if status == "downloaded":
                    downloaded += 1
                    print(f"✅ Downloaded {file_name}")
                elif status == "skipped":
                    skipped += 1
                    print(f"⏭️ Skipped {file_name}")
                else:
                    failed += 1
                    print(f"❌ Failed {file_name}: {details}")

        print(
            "📦 Download summary - "
            f"downloaded: {downloaded}, skipped: {skipped}, failed: {failed}"
        )

    def run(self):
        self.get_resources()
        self.scheduler.start()

if __name__ == "__main__":
    app = CScrapper()
    app.run()
