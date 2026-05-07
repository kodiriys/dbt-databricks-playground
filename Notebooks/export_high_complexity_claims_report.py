from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F

TARGET_COMPLEXITY = "high"
TARGET_DATE = datetime.now(ZoneInfo("America/Los_Angeles")) - timedelta(days=7)

df = spark.table("medallion.3_gold.fact_claim_complexity")

df_curated = (
    df.select("claim_id", "complexity_level", "total_complexity_score", "processed_at")
    .filter(
        (F.upper(df.complexity_level) == TARGET_COMPLEXITY.upper())
        & (df.processed_at >= TARGET_DATE)
    )
    .orderBy("total_complexity_score", ascending=False)
)

print("SELECT TOP 3")

df_curated.limit(3).show()

# Convert processed_at to date string, collect to pandas, render HTML table
import pandas as pd
from IPython.display import HTML, display
from pyspark.sql import functions as F

df_display = df_curated.withColumn(
    "processed_at", F.date_format("processed_at", "yyyy-MM-dd")
)

pdf = df_display.toPandas()

html = f"""
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@700;800&display=swap');

  .claim-wrapper {{
    font-family: 'DM Mono', monospace;
    background: #f8f7f4;
    border-radius: 12px;
    padding: 28px 32px;
    max-width: 860px;
    box-shadow: 0 0 0 1px #e2dfd6, 0 4px 20px rgba(0,0,0,0.06);
  }}

  .claim-title {{
    font-family: 'Syne', sans-serif;
    font-weight: 700;
    font-size: 13px;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: #c94f1a;
    margin: 0 0 20px 0;
  }}

  .claim-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }}

  .claim-table thead tr {{
    border-bottom: 1px solid #e2dfd6;
  }}

  .claim-table th {{
    text-align: left;
    color: #a09d96;
    font-weight: 500;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    font-size: 11px;
    padding: 0 16px 12px 0;
  }}

  .claim-table td {{
    padding: 13px 16px 13px 0;
    color: #2c2c2a;
    border-bottom: 1px solid #ede9e0;
    vertical-align: middle;
  }}

  .claim-table tbody tr:last-child td {{
    border-bottom: none;
  }}

  .claim-table tbody tr:hover td {{
    background: #f0ede6;
  }}

  .badge {{
    display: inline-block;
    background: #faece7;
    color: #993c1d;
    border: 1px solid #f0997b;
    border-radius: 4px;
    padding: 2px 8px;
    font-size: 11px;
    font-weight: 500;
    letter-spacing: 0.06em;
    text-transform: uppercase;
  }}

  .score {{
    font-weight: 500;
    color: #2c2c2a;
    background: #edeae2;
    border-radius: 4px;
    padding: 2px 8px;
    display: inline-block;
  }}

  .claim-id {{
    color: #888780;
    font-size: 12px;
  }}
</style>

<div class="claim-wrapper">
  <p class="claim-title">⬡ High Complexity Claims</p>
  <table class="claim-table">
    <thead>
      <tr>
        <th>Claim ID</th>
        <th>Complexity</th>
        <th>Score</th>
        <th>Processed</th>
      </tr>
    </thead>
    <tbody>
      {
    "".join(
        f'''
      <tr>
        <td><span class="claim-id">{row["claim_id"]}</span></td>
        <td><span class="badge">{row["complexity_level"]}</span></td>
        <td><span class="score">{row["total_complexity_score"]}</span></td>
        <td>{row["processed_at"]}</td>
      </tr>'''
        for _, row in pdf.iterrows()
    )
}
    </tbody>
  </table>
</div>
"""

display(HTML(html))
