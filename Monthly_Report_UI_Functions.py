import os
import pandas as pd
import boto3
import plotly.graph_objects as go
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dash import html
from shared_functions import conf, aws_conf

# Constants for Corridor Summary Table
LIGHT_BLUE = "#A6CEE3"
BLUE = "#1F78B4"
LIGHT_GREEN = "#B2DF8A"
GREEN = "#33A02C"
LIGHT_RED = "#FB9A99"
RED = "#E31A1C"
LIGHT_ORANGE = "#FDBF6F"
ORANGE = "#FF7F00"
LIGHT_PURPLE = "#CAB2D6"
PURPLE = "#6A3D9A"
LIGHT_BROWN = "#FFFF99"
BROWN = "#B15928"
DARK_GRAY = "#636363"
BLACK = "#000000"
DARK_GRAY_BAR = "#252525"
LIGHT_GRAY_BAR = "#bdbdbd"
RED2 = "#e41a1c"
GDOT_BLUE = "#045594"
GDOT_GREEN = "#13784B"
GDOT_YELLOW = "#EEB211"
SIGOPS_BLUE = "#00458F"
SIGOPS_GREEN = "#007338"

colrs = {
    "1": LIGHT_BLUE, "2": BLUE,
    "3": LIGHT_GREEN, "4": GREEN,
    "5": LIGHT_RED, "6": RED,
    "7": LIGHT_ORANGE, "8": ORANGE,
    "9": LIGHT_PURPLE, "10": PURPLE,
    "11": LIGHT_BROWN, "12": BROWN,
    "0": DARK_GRAY,
    "NA": DARK_GRAY,
    "Mainline": BLUE,
    "Passage": GREEN,
    "Demand": RED,
    "Queue": ORANGE,
    "Other": DARK_GRAY
}

RTOP1_ZONES = ["Zone 1", "Zone 2", "Zone 3", "Zone 8"]
RTOP2_ZONES = ["Zone 4", "Zone 5", "Zone 6", "Zone 7m", "Zone 7d"]

metric_order = ["du", "pau", "cctvu", "cu", "tp", "aog", "qs", "sf", "tti", "pti", "tasks"]
metric_names = [
    "Vehicle Detector Availability",
    "Ped Pushbutton Availability",
    "CCTV Availability",
    "Communications Uptime",
    "Traffic Volume (Throughput)",
    "Arrivals on Green",
    "Queue Spillback Rate",
    "Split Failure",
    "Travel Time Index",
    "Planning Time Index",
    "Outstanding Tasks"
]
metric_goals = ["> 95%", "> 95%", "> 95%", "> 95%", "< 5% dec. from prev. mo", "> 80%", "< 10%", "< 5%", "< 2.0", "< 2.0", "Dec. from prev. mo"]
metric_goals_numeric = [0.95, 0.95, 0.95, 0.95, -0.05, 0.8, 0.10, 0.05, 2, 2, 0]
metric_goals_sign = [">", ">", ">", ">", ">", ">", "<", "<", "<", "<", "<"]

yes_color = "green"
no_color = "red"

# Helper functions for formatting
def as_int(x):
    return f"{int(x):,}"

def as_2dec(x):
    return f"{x:.2f}"

def as_pct(x):
    return f"{x * 100:.1f}%"

def as_currency(x):
    return f"${x:,.2f}"

def data_format(data_type):
    return {
        "integer": as_int,
        "decimal": as_2dec,
        "percent": as_pct,
        "currency": as_currency
    }.get(data_type)

def tick_format(data_type):
    return {
        "integer": ",.0",
        "decimal": ".2f",
        "percent": ".0%",
        "currency": "$,.2f"
    }.get(data_type)

# Load configuration files

# Set AWS environment variables

os.environ["AWS_ACCESS_KEY_ID"] = aws_conf["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = aws_conf["AWS_SECRET_ACCESS_KEY"]
os.environ["AWS_DEFAULT_REGION"] = aws_conf["AWS_DEFAULT_REGION"]

conf["athena"]["uid"] = aws_conf["AWS_ACCESS_KEY_ID"]
conf["athena"]["pwd"] = aws_conf["AWS_SECRET_ACCESS_KEY"]

# Athena connection pool
def get_athena_connection_pool(conf):
    # Placeholder for Athena connection pool logic
    pass

athena_connection_pool = get_athena_connection_pool(conf["athena"])

# Date calculations
last_month = datetime.today() - timedelta(days=6)
last_month = last_month.replace(day=1)
end_of_last_month = last_month + relativedelta(months=1) - timedelta(days=1)
first_month = last_month - relativedelta(months=12)
report_months = pd.date_range(start=first_month, end=last_month, freq="MS")
month_options = report_months.strftime("%B %Y").tolist()

zone_group_options = conf["zone_groups"]

poll_interval = 1000 * 3600  # 3600 seconds = 1 hour

# AWS S3 check function
def s3_check_func(bucket, object_key):
    s3 = boto3.client("s3")
    def check():
        response = s3.list_objects_v2(Bucket=bucket, Prefix=object_key)
        return response["Contents"][0]["LastModified"] if "Contents" in response else None
    return check

# AWS S3 value function
def s3_value_func(bucket, object_key, aws_conf):
    s3 = boto3.client("s3", aws_access_key_id=aws_conf["AWS_ACCESS_KEY_ID"], aws_secret_access_key=aws_conf["AWS_SECRET_ACCESS_KEY"])
    def value():
        response = s3.get_object(Bucket=bucket, Key=object_key)
        return response["Body"].read()
    return value



# AWS S3 reactive poll function
def s3_reactive_poll(bucket, object_key, aws_conf, interval_seconds):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_conf["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=aws_conf["AWS_SECRET_ACCESS_KEY"]
    )

    def check():
        response = s3.list_objects_v2(Bucket=bucket, Prefix=object_key)
        return response["Contents"][0]["LastModified"] if "Contents" in response else None

    def value():
        response = s3.get_object(Bucket=bucket, Key=object_key)
        return response["Body"].read()

    return {"check": check, "value": value, "interval": interval_seconds}

# Example usage
poll_interval = 3600  # 1 hour
corridors_key = f"all_{conf['corridors_filename_s3']}.qs"
corridors = s3_reactive_poll(conf["bucket"], corridors_key, aws_conf, poll_interval)

alerts = s3_reactive_poll(conf["bucket"], "mark/watchdog/alerts.qs", aws_conf, poll_interval)

# Read zipped feather file
def read_zipped_feather(file_path):
    import pyarrow.feather as feather
    import zipfile

    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        extracted_file = zip_ref.namelist()[0]
        zip_ref.extract(extracted_file)
        return feather.read_feather(extracted_file)

# Read signal data from Athena
def read_signal_data(conn, signalid, plot_start_date, plot_end_date):
    query = f"""
        SELECT signalid, date, dat.hour, dat.detector, dat.callphase,
               dat.vol_rc, dat.vol_ac, dat.bad_day
        FROM signal_details, UNNEST(data) AS t(dat)
        WHERE signalid = '{signalid}'
          AND date >= '{plot_start_date}'
          AND date <= '{plot_end_date}'
        ORDER BY dat.detector, date, dat.hour
    """
    df = pd.read_sql(query, conn)
    df.fillna({"callphase": 0}, inplace=True)
    df["Timeperiod"] = pd.to_datetime(df["date"]) + pd.to_timedelta(df["hour"], unit="h")
    df["SignalID"] = df["signalid"].astype("category")
    df["Detector"] = df["detector"].astype("category")
    df["CallPhase"] = df["callphase"].astype("category")
    df["bad_day"] = df["bad_day"].astype(bool)
    return df

# Get last modified data
def get_last_modified(zmdf, zone=None, month=None):
    df = zmdf.groupby(["Month", "Zone"]).apply(lambda x: x[x["LastModified"] == x["LastModified"].max()]).reset_index(drop=True)
    if zone:
        df = df[df["Zone"] == zone]
    if month:
        df = df[df["Month"] == month]
    return df

# Read data from database
def read_from_db(conn):
    query = "SELECT * FROM progress_report_content"
    df = pd.read_sql(query, conn)
    df = df.groupby(["Month", "Zone"]).apply(lambda x: x.nlargest(10, "LastModified")).reset_index(drop=True)
    df["Month"] = pd.to_datetime(df["Month"]).dt.date
    df["LastModified"] = pd.to_datetime(df["LastModified"])
    return df

def get_valuebox(cor_monthly_df, var_, var_fmt, break_=False, zone=None, mo=None, qu=None):
    if qu is None:  # Monthly data
        vals = cor_monthly_df[(cor_monthly_df["Corridor"] == zone) & (cor_monthly_df["Month"] == mo)].to_dict(orient="records")[0]
    else:  # Quarterly data
        vals = cor_monthly_df[(cor_monthly_df["Corridor"] == zone) & (cor_monthly_df["Quarter"] == qu)].to_dict(orient="records")[0]

    vals["delta"] = vals.get("delta", 0)
    val = var_fmt(vals[var_])
    del_ = f"{' (+' if vals['delta'] > 0 else ' ('}{vals['delta']:.1%})"

    if break_:
        return html.Div([
            html.Div(val),
            html.P(del_, style={"font-size": "50%", "line-height": "5px", "padding": "0 0 0.2em 0"})
        ])
    else:
        return html.Div([
            html.Div(val),
            html.Span(del_, style={"font-size": "70%"})
        ])


def perf_plot_beta(data_, value_, name_, color_, fill_color_, format_func=lambda x: x, hoverformat_=",.0f", goal_=None):
    fig = go.Figure()

    if goal_ is not None:
        fig.add_trace(go.Scatter(
            x=data_["Month"], y=[goal_] * len(data_),
            mode="lines", name="Goal",
            line=dict(color="darkgray")
        ))
        fig.add_trace(go.Scatter(
            x=data_["Month"], y=data_[value_],
            mode="lines", name=name_,
            line=dict(color=color_),
            fill="tonexty", fillcolor=fill_color_
        ))
    else:
        fig.add_trace(go.Scatter(
            x=data_["Month"], y=data_[value_],
            mode="lines", name=name_,
            line=dict(color=color_)
        ))

    first = data_.iloc[0]
    last = data_.iloc[-1]
    fig.add_annotation(
        x=0, y=first[value_],
        text=format_func(first[value_]),
        showarrow=False, xanchor="right", xref="paper", width=50, align="right", borderpad=5
    )
    fig.add_annotation(
        x=1, y=last[value_],
        text=format_func(last[value_]),
        showarrow=False, xanchor="left", xref="paper", width=60, align="left", borderpad=5
    )
    fig.update_layout(
        xaxis=dict(title="", showticklabels=True, showgrid=False),
        yaxis=dict(title="", showticklabels=False, showgrid=False, zeroline=False, hoverformat=hoverformat_),
        showlegend=False,
        margin=dict(l=50, r=60, t=10, b=10)
    )
    return fig

def perf_plot(data_, value_, name_, color_, format_func=lambda x: x, hoverformat_=",.0f", goal_=None):
    fig = go.Figure()

    if goal_ is not None:
        fig.add_trace(go.Scatter(
            x=data_["Month"], y=[goal_] * len(data_),
            mode="lines", name="Goal",
            line=dict(color="lightred", dash="dot")
        ))
    fig.add_trace(go.Scatter(
        x=data_["Month"], y=data_[value_],
        mode="lines+markers", name=name_,
        line=dict(color=color_),
        marker=dict(size=8, color=color_, line=dict(width=1, color="rgba(255, 255, 255, 0.8)"))
    ))

    first = data_.iloc[0]
    last = data_.iloc[-1]
    fig.add_annotation(
        x=first["Month"], y=first[value_],
        text=format_func(first[value_]),
        showarrow=False, xanchor="right", xshift=-10
    )
    fig.add_annotation(
        x=last["Month"], y=last[value_],
        text=format_func(last[value_]),
        showarrow=False, xanchor="left", xshift=20
    )
    fig.update_layout(
        xaxis=dict(title="", showticklabels=True, showgrid=False),
        yaxis=dict(title="", showticklabels=False, showgrid=False, zeroline=False, hoverformat=hoverformat_),
        showlegend=False,
        margin=dict(l=120, r=40)
    )
    return fig

def no_data_plot(name_):
    fig = go.Figure()
    fig.update_layout(
        xaxis=dict(title="", showticklabels=False, showgrid=False, zeroline=False),
        yaxis=dict(title="", showticklabels=False, showgrid=False, zeroline=False),
        annotations=[
            dict(x=-0.02, y=0.4, xref="paper", yref="paper", xanchor="right", text=name_, font=dict(size=12), showarrow=False),
            dict(x=0.5, y=0.5, xref="paper", yref="paper", text="NO DATA", font=dict(size=16), showarrow=False)
        ],
        margin=dict(l=180, r=100)
    )
    return fig


# Empty plot - space filler
def empty_plot():
    x0 = dict(zeroline=False, ticks="", showticklabels=False, showgrid=False)
    fig = go.Figure()
    fig.update_layout(xaxis=x0, yaxis=x0)
    return fig

# Dashboard plot with bar and line charts
def get_bar_line_dashboard_plot(cor_weekly, cor_monthly, cor_hourly=None, var_=None, num_format=None,
                                 highlight_color=None, month_=None, zone_group_=None, x_bar_title="___",
                                 x_line1_title="___", x_line2_title="___", plot_title="___",
                                 goal=None, accent_average=True):
    # Format functions based on num_format
    if num_format == "percent":
        var_fmt = lambda x: f"{x:.1%}"
        tickformat_ = ".0%"
    elif num_format == "integer":
        var_fmt = lambda x: f"{int(x):,}"
        tickformat_ = ",.0"
    elif num_format == "decimal":
        var_fmt = lambda x: f"{x:.2f}"
        tickformat_ = ".2f"

    # Filter data
    mdf = cor_monthly[cor_monthly["Zone_Group"] == zone_group_]
    wdf = cor_weekly[cor_weekly["Zone_Group"] == zone_group_]

    mdf = mdf[mdf["Month"] == month_]
    wdf = wdf[wdf["Date"] < pd.Timestamp(month_) + pd.offsets.MonthEnd(1)]

    if not mdf.empty and not wdf.empty:
        # Current Month Data
        mdf = mdf.sort_values(var_).assign(var=mdf[var_])
        mdf["Corridor"] = pd.Categorical(mdf["Corridor"], categories=mdf["Corridor"])
        mdf["col"] = mdf["Corridor"].apply(lambda x: highlight_color if x == zone_group_ else "lightgray")

        # Bar chart
        bar_chart = go.Figure()
        bar_chart.add_trace(go.Bar(
            x=mdf["var"],
            y=mdf["Corridor"],
            orientation="h",
            marker=dict(color=mdf["col"]),
            text=mdf["var"].apply(var_fmt),
            textposition="auto",
            name=""
        ))
        bar_chart.update_layout(
            barmode="overlay",
            xaxis=dict(title=x_bar_title, zeroline=False, tickformat=tickformat_),
            yaxis=dict(title=""),
            showlegend=False,
            margin=dict(pad=4, l=100)
        )
        if goal:
            bar_chart.add_trace(go.Scatter(
                x=[goal] * len(mdf),
                y=mdf["Corridor"],
                mode="lines",
                line=dict(color="red", dash="dot"),
                name="Goal"
            ))

        # Weekly Data - historical trend
        wdf["var"] = wdf[var_]
        wdf["col"] = wdf["Corridor"].apply(lambda x: 0 if x == zone_group_ else 1)

        weekly_line_chart = go.Figure()
        weekly_line_chart.add_trace(go.Scatter(
            x=wdf["Date"],
            y=wdf["var"],
            mode="lines",
            line=dict(color="black", width=2),
            name=""
        ))
        weekly_line_chart.update_layout(
            xaxis=dict(title=x_line1_title, tickformat=tickformat_),
            yaxis=dict(title="", hoverformat=tickformat_),
            showlegend=False,
            margin=dict(t=50)
        )

        # Hourly Data - current month
        if cor_hourly is not None:
            hdf = cor_hourly[cor_hourly["Zone_Group"] == zone_group_]
            hdf = hdf[hdf["Hour"].dt.date == month_]
            hdf["var"] = hdf[var_]
            hdf["col"] = hdf["Corridor"].apply(lambda x: 0 if x == zone_group_ else 1)

            hourly_line_chart = go.Figure()
            hourly_line_chart.add_trace(go.Scatter(
                x=hdf["Hour"],
                y=hdf["var"],
                mode="lines",
                line=dict(color="black", width=2),
                name=""
            ))
            hourly_line_chart.update_layout(
                xaxis=dict(title=x_line2_title, tickformat=tickformat_),
                yaxis=dict(title="", hoverformat=tickformat_),
                showlegend=False
            )

            # Combine weekly and hourly charts
            s1 = go.Figure()
            s1.add_trace(weekly_line_chart.data[0])
            s1.add_trace(hourly_line_chart.data[0])
        else:
            s1 = weekly_line_chart

        # Combine bar chart and line charts
        final_plot = go.Figure()
        final_plot.add_trace(bar_chart.data[0])
        final_plot.add_trace(s1.data[0])
        final_plot.update_layout(
            title=plot_title,
            margin=dict(l=100),
            highlight=dict(color=highlight_color, opacityDim=0.9)
        )
        return final_plot
    else:
        return empty_plot()