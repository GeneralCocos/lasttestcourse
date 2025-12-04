import csv
import random
from datetime import datetime, timedelta

NUM_ROWS = 1000
NUM_COLS = 50
OUT_FILE = "input.csv"


def random_date():
    base = datetime(2020, 1, 1)
    delta = timedelta(days=random.randint(0, 365 * 3))
    return (base + delta).isoformat(sep=" ")


def main():
    fieldnames = ["id"] + [f"col_{i:02d}" for i in range(1, NUM_COLS)]
    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(1, NUM_ROWS + 1):
            row = {"id": i}
            for j in range(1, NUM_COLS):
                # немного разнообразия типов
                if j % 5 == 0:
                    row[f"col_{j:02d}"] = random_date()
                elif j % 5 == 1:
                    row[f"col_{j:02d}"] = random.randint(0, 1_000_000)
                elif j % 5 == 2:
                    row[f"col_{j:02d}"] = round(random.uniform(0, 1000), 4)
                elif j % 5 == 3:
                    row[f"col_{j:02d}"] = random.choice(["A", "B", "C", "D"])
                else:
                    row[f"col_{j:02d}"] = f"text_{random.randint(1, 10_000)}"
            writer.writerow(row)


if __name__ == "__main__":
    main()
