from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch


OUTPUT_PATH = Path("docs/architecture.png")


def add_box(ax, x, y, w, h, text, facecolor="#F5F7FA", edgecolor="#2F3B52"):
    box = FancyBboxPatch(
        (x, y),
        w,
        h,
        boxstyle="round,pad=0.02,rounding_size=0.03",
        linewidth=1.5,
        edgecolor=edgecolor,
        facecolor=facecolor,
    )
    ax.add_patch(box)
    ax.text(
        x + w / 2,
        y + h / 2,
        text,
        ha="center",
        va="center",
        fontsize=11,
        color="#1F2937",
        wrap=True,
    )


def add_arrow(ax, start, end, color="#4B5563"):
    arrow = FancyArrowPatch(
        start,
        end,
        arrowstyle="->",
        mutation_scale=14,
        linewidth=1.8,
        color=color,
    )
    ax.add_patch(arrow)


def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(16, 9))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    ax.text(
        0.5,
        0.95,
        "RetailStream Architecture",
        ha="center",
        va="center",
        fontsize=18,
        fontweight="bold",
        color="#111827",
    )

    # Batch ingestion path
    add_box(ax, 0.05, 0.68, 0.16, 0.10, "Olist CSVs", facecolor="#E8F1FB")
    add_box(ax, 0.28, 0.68, 0.18, 0.10, "Python Loader", facecolor="#EAF7EE")
    add_box(ax, 0.54, 0.68, 0.21, 0.10, "Bronze Layer\n(DuckDB)", facecolor="#FFF4E5")

    add_arrow(ax, (0.21, 0.73), (0.28, 0.73))
    add_arrow(ax, (0.46, 0.73), (0.54, 0.73))

    # Streaming ingestion path
    add_box(ax, 0.05, 0.46, 0.16, 0.10, "Olist CSVs", facecolor="#E8F1FB")
    add_box(ax, 0.25, 0.46, 0.18, 0.10, "Kafka Producer", facecolor="#F3E8FF")
    add_box(ax, 0.47, 0.46, 0.16, 0.10, "Kafka Topic", facecolor="#FCE7F3")
    add_box(ax, 0.67, 0.46, 0.18, 0.10, "Kafka Consumer", facecolor="#F3E8FF")

    add_arrow(ax, (0.21, 0.51), (0.25, 0.51))
    add_arrow(ax, (0.43, 0.51), (0.47, 0.51))
    add_arrow(ax, (0.63, 0.51), (0.67, 0.51))
    add_arrow(ax, (0.76, 0.56), (0.67, 0.68))

    # Transformation and analytics path
    add_box(ax, 0.10, 0.20, 0.18, 0.10, "Bronze Layer", facecolor="#FFF4E5")
    add_box(ax, 0.34, 0.20, 0.18, 0.10, "dbt Staging /\nSilver", facecolor="#EAF7EE")
    add_box(ax, 0.58, 0.20, 0.18, 0.10, "dbt Gold\nDim / Fact", facecolor="#E8F1FB")
    add_box(ax, 0.79, 0.20, 0.16, 0.10, "dbt Reporting\nMarts", facecolor="#FCE7F3")
    add_box(ax, 0.79, 0.04, 0.16, 0.10, "KPI Analysis", facecolor="#F3F4F6")

    add_arrow(ax, (0.28, 0.25), (0.34, 0.25))
    add_arrow(ax, (0.52, 0.25), (0.58, 0.25))
    add_arrow(ax, (0.76, 0.25), (0.79, 0.25))
    add_arrow(ax, (0.87, 0.20), (0.87, 0.14))

    # Connect Bronze from ingestion layers into analytics path
    add_arrow(ax, (0.645, 0.68), (0.19, 0.30))

    ax.text(
        0.5,
        0.86,
        "Batch Ingestion",
        ha="center",
        va="center",
        fontsize=12,
        fontweight="bold",
        color="#374151",
    )
    ax.text(
        0.5,
        0.61,
        "Streaming Ingestion Demo",
        ha="center",
        va="center",
        fontsize=12,
        fontweight="bold",
        color="#374151",
    )
    ax.text(
        0.5,
        0.36,
        "Transformation and Analytics",
        ha="center",
        va="center",
        fontsize=12,
        fontweight="bold",
        color="#374151",
    )

    plt.tight_layout()
    plt.savefig(OUTPUT_PATH, dpi=200, bbox_inches="tight")
    plt.close(fig)

    print(f"Architecture image saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
