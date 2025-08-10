# Fixed viz.py with proper pie chart "Others" handling
import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import io, os, re, requests
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import colorsys, random

client = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))

def _distinct_colors(n):
    """Golden-ratio color palette for visually distinct, vibrant colors."""
    golden = 0.61803398875
    h = random.random()  # random starting hue
    colors = []
    for _ in range(n):
        h = (h + golden) % 1
        r, g, b = colorsys.hsv_to_rgb(h, 0.6, 0.95)  # vibrant tones
        colors.append(f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}")
    return colors


def _row_to_dict(row):
    if isinstance(row, dict):
        return row
    if isinstance(row, (tuple, list)):
        return {f"col{idx}": val for idx, val in enumerate(row)}
    raise ValueError("Row must be dict/tuple/list")

def _suggest_chart_type(rows):
    if not rows:
        return "bar"
    sample = _row_to_dict(rows[0])
    cols = list(sample.keys())
    label_key = next((c for c in cols if isinstance(sample[c], str)), cols[0])
    value_key = next((c for c in cols if isinstance(sample[c], (int, float))), cols[min(1, len(cols)-1)])

    values = [float(_row_to_dict(r).get(value_key, 0)) for r in rows]
    labels = [_row_to_dict(r).get(label_key, "") for r in rows]

    if len(rows) <= 10 and len(set(values)) == len(values):
        return "pie"

    if any(re.search(r"\d{4}|\d{2}/\d{2}|jan|feb|mon|tue|202[0-9]", l, re.I) for l in labels):
        return "line"

    return "bar"

def _render_chart(rows, chart_type="bar", title="Chart"):
    rows_dict = [_row_to_dict(r) for r in rows]
    sample = rows_dict[0]
    cols = list(sample.keys())

    label_key = next((c for c in cols if isinstance(sample[c], str)), cols[0])
    value_key = next((c for c in cols if isinstance(sample[c], (int, float))), cols[min(1, len(cols)-1)])

    x = [r.get(label_key, "N/A") for r in rows_dict]
    y = [float(r.get(value_key, 0)) for r in rows_dict]

    colors = _distinct_colors(len(x))

    plt.figure(figsize=(10, 6))
    
    if chart_type == "bar":
        plt.bar(x, y, color=colors)
        plt.xlabel(label_key)
        plt.ylabel(value_key)
        plt.title(f"Bar Chart: {value_key} by {label_key}")
        plt.xticks(rotation=45, ha="right")
        
    elif chart_type == "line":
        plt.plot(x, y, color="#007ACC", linewidth=2.5, marker="o", markersize=6)
        plt.xlabel(label_key)
        plt.ylabel(value_key)
        plt.title(f"Line Chart: {value_key} by {label_key}")
        plt.xticks(rotation=45, ha="right")
        plt.grid(True, alpha=0.3)
        
    elif chart_type == "pie":
        total = sum(y)
        
        # Enhanced pie chart with better "Others" handling
        labels_to_show = []
        values_to_show = []
        colors_to_show = []
        
        # Calculate percentages and prepare labels
        for i, (label, value) in enumerate(zip(x, y)):
            percentage = (value / total) * 100
            
            # Show percentage in label for all slices
            if "Others" in label:
                # For "Others" slice, show the actual count in parentheses
                others_match = re.search(r'Others \((\d+) items\)', label)
                if others_match:
                    others_count = others_match.group(1)
                    display_label = f"Others ({others_count} items)\n{percentage:.1f}%"
                else:
                    display_label = f"{label}\n{percentage:.1f}%"
            else:
                # For regular slices, show label and percentage
                display_label = f"{label}\n{percentage:.1f}%"
            
            labels_to_show.append(display_label)
            values_to_show.append(value)
            colors_to_show.append(colors[i])
        
        # Create pie chart with enhanced styling
        wedges, texts, autotexts = plt.pie(
            values_to_show,
            labels=labels_to_show,
            colors=colors_to_show,
            autopct='',  # We'll handle percentages in labels
            startangle=90,
            textprops={"fontsize": 9, "weight": "bold"},
            pctdistance=0.85
        )
        
        # Improve text visibility
        for text in texts:
            text.set_color("black")
            text.set_fontweight("bold")
        
        plt.title(f"Pie Chart: {title}", fontsize=12, fontweight="bold", pad=20)
        
        # Equal aspect ratio ensures that pie is drawn as a circle
        plt.axis('equal')
        
    else:
        raise ValueError("Invalid chart type")

    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=300, bbox_inches='tight')
    buf.seek(0)
    plt.close()  # Important: close the figure to free memory
    return buf

def chart_to_slack(rows, channel, title="Chart", chart_type=None, thread_ts=None):
    """
    Updated function to support posting in threads
    
    Args:
        rows: Chart data
        channel: Slack channel
        title: Chart title
        chart_type: Type of chart (optional)
        thread_ts: Thread timestamp for posting in thread (optional)
    """
    if not rows:
        return

    try:
        chart_type = chart_type or _suggest_chart_type(rows)
        buf = _render_chart(rows, chart_type, title)

        # Send interactive buttons (now supports threads)
        buttons_msg = client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,  # Added thread support
            text="📊 Ready to generate your chart!",
            blocks=[
                {"type": "section", "text": {
                    "type": "mrkdwn",
                    "text": (
                        f":star: *{chart_type.capitalize()} chart recommended*\n"
                    )
                }},
                {"type": "actions", "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "📊 Bar"},
                        "value": f"bar|{title}",
                        "action_id": "toggle_chart_bar"
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "📈 Line"},
                        "value": f"line|{title}",
                        "action_id": "toggle_chart_line"
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "🥧 Pie"},
                        "value": f"pie|{title}",
                        "action_id": "toggle_chart_pie"
                    },
                ]}
            ]
        )

        buttons_ts = buttons_msg["ts"]
        return buttons_ts

    except SlackApiError as e:
        print(f"❌ Slack API error: {e.response['error']}")
    except Exception as e:
        print(f"❌ General error: {e}")