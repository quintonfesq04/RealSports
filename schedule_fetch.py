#!/usr/bin/env python3
from app.schedule_service import refresh_schedule, SCHEDULE_JSON


def main() -> None:
    rows = refresh_schedule()
    print(f"[schedule] wrote {len(rows)} entries to {SCHEDULE_JSON}")


if __name__ == "__main__":
    main()
