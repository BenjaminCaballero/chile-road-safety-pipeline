import requests
import logging

logger = logging.getLogger(__name__)

BASE_URL = (
    "https://www.observatoriologistico.cl"
    "/api/v1/datastreams/C015/data"
)

TIMEOUT = 15


def fetch_page(page: int, limit: int = 50) -> dict:

    response = requests.get(
        BASE_URL,
        params={
            "page": page,
            "limit": limit
        },
        timeout=TIMEOUT
    )

    response.raise_for_status()

    payload = response.json()

    if payload.get("status") != "success":
        raise ValueError(
            f"API returned an unsuccessful status on page {page}"
        )

    return payload

def fetch_all_records() -> list[dict]:

    first_page = fetch_page(1)

    pagination = first_page["pagination"]

    total_pages = pagination["total_pages"]
    expected_total = pagination["total"]

    all_records = []

    for page in range(1, total_pages + 1):

        if page == 1:
            payload = first_page
        else:
            payload = fetch_page(page)

        current_pagination = payload["pagination"]

        if (
            current_pagination["page"] != page
            or current_pagination["total_pages"] != total_pages
            or current_pagination["total"] != expected_total
        ):
            raise ValueError(
                f"Inconsistent pagination on page {page}"
            )

        page_records = payload["data"]

        if not isinstance(page_records, list):
            raise ValueError(
                f"Invalid data format on page {page}"
            )

        all_records.extend(page_records)

        print(
            f"Page {page}/{total_pages}: "
            f"{len(page_records)} records"
        )

    if len(all_records) != expected_total:
        raise ValueError(
            f"Expected {expected_total} records, "
            f"but received {len(all_records)}"
        )

    return all_records

if __name__ == "__main__":

    records = fetch_all_records()

    print(f"\nTotal records: {len(records)}")

    print("\nFirst record:")
    print(records[0])

    print("\nLast record:")
    print(records[-1])