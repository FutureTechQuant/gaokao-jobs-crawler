import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from crawlers.plans import PlanCrawler


def write_output(path: str, key: str, value):
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{key}={value}\n")


def load_school_ids(shard: str):
    schools_file = PROJECT_ROOT / "data" / "schools.json"
    if not schools_file.exists():
        raise FileNotFoundError(f"未找到学校文件: {schools_file}")

    with open(schools_file, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if isinstance(payload, list):
        schools = payload
    elif isinstance(payload, dict):
        schools = payload.get("data", [])
    else:
        schools = []

    school_ids = []
    for item in schools:
        if isinstance(item, dict) and item.get("school_id"):
            school_ids.append(str(item["school_id"]))

    def sort_key(x):
        return (0, int(x)) if x.isdigit() else (1, x)

    school_ids = sorted(dict.fromkeys(school_ids), key=sort_key)

    midpoint = (len(school_ids) + 1) // 2
    if shard == "upper":
        school_ids = school_ids[:midpoint]
    elif shard == "lower":
        school_ids = school_ids[midpoint:]
    elif shard != "all":
        raise ValueError(f"无效分片: {shard}")

    sample_count = int(os.getenv("SAMPLE_SCHOOLS", "0") or 0)
    if sample_count > 0:
        school_ids = school_ids[:sample_count]

    return school_ids


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--shard", required=True)
    parser.add_argument("--mode", required=True)
    parser.add_argument("--github-output", default="")
    args = parser.parse_args()

    school_ids = load_school_ids(args.shard)

    if not school_ids:
        write_output(args.github_output, "run_year", args.year)
        write_output(args.github_output, "run_shard", args.shard)
        write_output(args.github_output, "run_status", "skipped")
        write_output(args.github_output, "saved_documents", 0)
        write_output(args.github_output, "completed_schools", 0)
        print({"year": args.year, "status": "skipped"})
        return

    crawler = PlanCrawler()

    # 旧版 plans.py 不支持 mode 参数
    result = crawler.crawl(
        school_ids=school_ids,
        years=args.year,
    )

    year_key = str(args.year)
    year_records = result.get(year_key, []) if isinstance(result, dict) else []

    write_output(args.github_output, "run_year", args.year)
    write_output(args.github_output, "run_shard", args.shard)
    write_output(args.github_output, "run_status", "done")
    write_output(args.github_output, "saved_documents", len(year_records))
    write_output(args.github_output, "completed_schools", len(school_ids))

    print({
        "year": args.year,
        "shard": args.shard,
        "status": "done",
        "saved_documents": len(year_records),
        "completed_schools": len(school_ids),
    })


if __name__ == "__main__":
    main()
