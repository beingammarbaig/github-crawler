import requests, time, os, sys
from dotenv import load_dotenv
from db import init_schema, upsert_repo, dump_to_csv

load_dotenv()
GITHUB_GRAPHQL = "https://api.github.com/graphql"

def run_crawl(query, max_repos=1000):
    headers = {"Authorization": f"bearer {os.environ['GITHUB_TOKEN']}"}
    cursor, fetched = None, 0

    while fetched < max_repos:
        q = f"""
        query {{
          search(query: "{query}", type: REPOSITORY, first: 100, after: {('"%s"' % cursor) if cursor else 'null'}) {{
            pageInfo {{ endCursor hasNextPage }}
            nodes {{
              ... on Repository {{
                id
                name
                owner {{ login }}
                stargazerCount
                url
                createdAt
                updatedAt
              }}
            }}
          }}
          rateLimit {{ remaining resetAt }}
        }}
        """

        r = requests.post(GITHUB_GRAPHQL, headers=headers, json={"query": q})
        data = r.json()

        if "errors" in data:
            print("❌ GitHub API error:", data["errors"])
            time.sleep(5)
            continue

        limit = data.get("data", {}).get("rateLimit", {})
        if limit and limit.get("remaining", 1) < 5:
            wait_for = 60
            print(f"⏳ Rate limited. Sleeping for {wait_for} seconds...")
            time.sleep(wait_for)
            continue

        search_data = data.get("data", {}).get("search", {})
        nodes = search_data.get("nodes", [])

        for node in nodes:
            upsert_repo(
                node["id"],
                node["name"],
                node["owner"]["login"],
                node["stargazerCount"]
            )
            print(f"✅ Crawled {fetched} repositories")
            fetched += 1
            if fetched >= max_repos:
                break

        page_info = search_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break

        cursor = page_info.get("endCursor")

    print(f"✅ Crawled {fetched} repositories")

if __name__ == "__main__":
    print("🚀 Initializing database schema...")
    init_schema()
    print("🔍 Starting GitHub crawl...")
    run_crawl("stars:>100 language:Python created:2023-01-01..2023-12-31", max_repos=10)
    print("📦 Dumping data to CSV...")
    dump_to_csv()
    print("🎉 Done!")
