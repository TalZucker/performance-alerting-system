import requests
import json
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from pyspark.sql.window import Window

class AlertsManager:

    def __init__(self, spark_session):
        """
        Initializes the AlertsManager by loading data and setting up configurations.
        """
        self.spark = spark_session

        # API Call for Project Data
        self.api_key = "REDACTED_SECURE_TOKEN" 
        self.config_url = "https://api.internal-services.com/v1/config/vertical-settings"

        try:
            response = requests.post(
                self.config_url,
                headers={"x-api-key": self.api_key},
            )
            response.raise_for_status()
            self.projects_data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"ERROR: Failed to fetch project configuration: {e}")
            self.projects_data = {}

        # Load Gold Tables
        print("INFO: Loading and caching gold tables...")
        self.daily_actions_funnel = self.spark.table(f"{SCHEMA}.gold.daily_actions_funnel").cache()
        self.daily_ad_performance_metrics = self.spark.table(f"{SCHEMA}.gold.daily_ad_performance_metrics").cache()
        self.hourly_ad_performance_metrics = self.spark.table(f"{SCHEMA}.gold.hourly_ad_performance_metrics").cache()
        self.sessions_full_lifecycle = self.spark.table(f"{SCHEMA}.gold.sessions_full_lifecycle").cache()
        self.sessions_info = self.spark.table(f"{SCHEMA}.gold.sessions_info").cache()
        print("INFO: Gold tables loaded and cached successfully.")

        # Centralized configuration parameters
        self.config = {
            "cpa_mad_scaling_factor": 1.4826,
            "cpa_mad_threshold_multiplier": 2,
            "performance_decline_drop_factor": 0.9,
            "fb_to_clickout_drop_factor": 0.9,
            "clickout_to_conversion_factor": 0.90,
            "lp_abandonment_benchmark_factor": 1.03,
            "volume_drop_percentage": 0.2,
            "flow_interruption_automated_multiplier": 2.0,
            "flow_interruption_min_historical_gaps": 5,
            "min_spend_for_cpa_alert": 0,
            "min_spend_for_roi_alert": 0,
            "min_clicks_for_fb_to_clickout": 1,
            "min_clickouts_for_cr_alert": 1,
            "min_sessions_for_lp_abandonment": 1
        }
    
    ### Helper Functions for Weighted Calculations
    def _weighted_median_candidate_expr(self, value_col, weight_col, order_window_spec, total_weight_window_spec):
        total_weight = F.sum(weight_col).over(total_weight_window_spec)
        cumulative_weight = F.sum(weight_col).over(order_window_spec)
        median_boundary = total_weight / 2.0
        return F.when(
            (cumulative_weight >= median_boundary) &
            ((cumulative_weight - F.col(weight_col) < median_boundary) | (F.col(weight_col) == median_boundary)),
            F.col(value_col)
        ).otherwise(F.lit(None))

    def _weighted_mad_candidate_expr(self, value_col, weight_col, median_col_name, order_window_spec, total_weight_window_spec):
        absolute_deviation = F.abs(F.col(value_col) - F.col(median_col_name))
        total_weight_dev = F.sum(weight_col).over(total_weight_window_spec)
        cumulative_weight_dev = F.sum(weight_col).over(order_window_spec)
        mad_boundary = total_weight_dev / 2.0
        return F.when(
            (cumulative_weight_dev >= mad_boundary) &
            ((cumulative_weight_dev - F.col(weight_col) < mad_boundary) | (F.col(weight_col) == mad_boundary)),
            absolute_deviation
        ).otherwise(F.lit(None))
    
    ### Alerts check methods
    def check_volume_drop(self, data_smm_hourly, data_sessions_full_lifecycle, vertical_name, base_threshold, mode, triggered_alerts_list, campaign_name_map, percent_increase=0):
        min_daily_spend_for_alert = self.config["min_spend_for_cpa_alert"]
        hourly_conversions = data_sessions_full_lifecycle.filter(
            (F.col("conversion_dt") >= F.expr("current_timestamp() - interval 1 hour")) &
            (F.col("conversion_dt").isNotNull())
        )
        current_hour_volume = hourly_conversions.count()

        daily_spend_today_row = data_smm_hourly.filter(
            F.to_date("timestamp") == F.current_date()
        ).agg(F.sum("spend").alias("daily_spend")).first()
        daily_spend_today = daily_spend_today_row["daily_spend"] if daily_spend_today_row and daily_spend_today_row["daily_spend"] is not None else 0

        if mode in ["campaign_controlled", "project_controlled"]:
            effective_threshold = base_threshold
            if percent_increase and percent_increase > 0:
                effective_threshold = base_threshold * (1 + percent_increase / 100)
            if current_hour_volume < effective_threshold and daily_spend_today > min_daily_spend_for_alert:
                triggered_alerts_list.append({
                    "alert_name": "volumeDrop", "vertical_name": vertical_name,
                    "campaign_name": "N/A", "campaign_id": "N/A",
                    "calculated_value": current_hour_volume, "calculated_value_timeframe": "Last 1 Hour",
                    "benchmark": effective_threshold, "benchmark_timeframe": "Controlled Threshold",
                    "landing_page": "N/A", "spend": daily_spend_today, "leads" : "N/A",
                    "base_threshold": base_threshold
                })
        elif mode == "automated_benchmark":
            drop_percentage = self.config["volume_drop_percentage"]
            historical_same_hour_data = data_sessions_full_lifecycle.filter(
                (F.to_date(F.col("conversion_dt")) >= F.expr("current_date() - interval 7 day")) &
                (F.to_date(F.col("conversion_dt")) < F.current_date()) &
                (F.hour(F.col("conversion_dt")) == F.hour(F.current_timestamp())) &
                (F.col("conversion_dt").isNotNull())
            )
            daily_counts_for_hour = historical_same_hour_data.groupBy(F.to_date(F.col("conversion_dt"))).count()
            average_historical_volume_same_hour_row = daily_counts_for_hour.agg(F.avg("count")).first()
            average_historical_volume_same_hour = average_historical_volume_same_hour_row[0] if average_historical_volume_same_hour_row and average_historical_volume_same_hour_row[0] is not None else 0
            benchmark_threshold = average_historical_volume_same_hour * (1 - drop_percentage)
            if (average_historical_volume_same_hour > 0 and 
                current_hour_volume < benchmark_threshold and
                daily_spend_today > min_daily_spend_for_alert):
                triggered_alerts_list.append({
                    "alert_name": "volumeDrop", "vertical_name": vertical_name,
                    "campaign_name": "N/A", "campaign_id": "N/A",
                    "calculated_value": current_hour_volume, "calculated_value_timeframe": "Last 1 Hour",
                    "benchmark": benchmark_threshold, "benchmark_timeframe": "Last 7 Days (same hour avg)",
                    "landing_page": "N/A", "spend": daily_spend_today, "leads" : "N/A",
                    "base_threshold": base_threshold
                })

    def check_flow_interruption(self, data_smm_hourly, data_sessions_full_lifecycle, vertical_name, base_threshold, mode, triggered_alerts_list, campaign_name_map, percent_increase=0):
        min_daily_spend_for_alert = self.config["min_spend_for_cpa_alert"]
        max_conversion_dt_row = data_sessions_full_lifecycle.filter(
            F.col("conversion_dt").isNotNull()
        ).agg(F.max("conversion_dt")).first()
        last_activity_timestamp = max_conversion_dt_row[0] if max_conversion_dt_row and max_conversion_dt_row[0] is not None else None
        time_diff_minutes = float('inf')
        if last_activity_timestamp:
            current_time_utc = datetime.utcnow()
            time_diff_minutes = (current_time_utc - last_activity_timestamp).total_seconds() // 60
        daily_spend_today_row = data_smm_hourly.filter(
            F.to_date("timestamp") == F.current_date()
        ).agg(F.sum("spend").alias("daily_spend")).first()
        daily_spend_today = daily_spend_today_row["daily_spend"] if daily_spend_today_row and daily_spend_today_row["daily_spend"] is not None else 0
        
        if mode in ["campaign_controlled", "project_controlled"]:
            effective_threshold = base_threshold
            if percent_increase and percent_increase > 0:
                effective_threshold = base_threshold * (1 + percent_increase / 100)
            if daily_spend_today > min_daily_spend_for_alert and time_diff_minutes > effective_threshold:
                triggered_alerts_list.append({
                    "alert_name": "flowInterruption", "vertical_name": vertical_name,
                    "campaign_name": "N/A", "campaign_id": "N/A",
                    "calculated_value": time_diff_minutes, "calculated_value_timeframe": "Minutes since last Click-out/Lead",
                    "benchmark": effective_threshold, "benchmark_timeframe": "Controlled Threshold (minutes)",
                    "landing_page": "N/A", "spend": daily_spend_today, "leads" : "N/A",
                    "base_threshold": base_threshold
                })
        elif mode == "automated_benchmark":
            min_historical_gaps_for_benchmark = self.config["flow_interruption_min_historical_gaps"]
            automated_multiplier = self.config["flow_interruption_automated_multiplier"]
            historical_conversions_df = data_sessions_full_lifecycle.filter(
                    (F.col("conversion_dt") >= F.expr("current_timestamp() - interval 4 weeks")) &
                    (F.col("conversion_dt") < F.to_timestamp(F.current_date())) &
                    (F.col("conversion_dt").isNotNull())
                ).select("conversion_dt").orderBy("conversion_dt")
            window_spec = Window.orderBy("conversion_dt")
            historical_gaps_df = historical_conversions_df.withColumn(
                "prev_timestamp", F.lag("conversion_dt", 1).over(window_spec)
            ).filter(F.col("prev_timestamp").isNotNull()).withColumn(
                "gap_minutes", (F.col("conversion_dt").cast("long") - F.col("prev_timestamp").cast("long")) / 60
            )
            max_relevant_gap_minutes = 720
            relevant_gaps_df = historical_gaps_df.filter(F.col("gap_minutes") <= max_relevant_gap_minutes)
            median_and_count_row = relevant_gaps_df.agg( 
                F.percentile_approx("gap_minutes", 0.5).alias("median_gap_minutes"),
                F.count("gap_minutes").alias("num_gaps_for_median")
            ).first()
            median_historical_inter_conversion_gap_minutes = 0
            num_historical_gaps_for_benchmark = 0 
            if median_and_count_row:
                if median_and_count_row["median_gap_minutes"] is not None:
                    median_historical_inter_conversion_gap_minutes = median_and_count_row["median_gap_minutes"]
                num_historical_gaps_for_benchmark = median_and_count_row["num_gaps_for_median"]
            automated_dynamic_threshold = max(5, median_historical_inter_conversion_gap_minutes * automated_multiplier)
            if (num_historical_gaps_for_benchmark >= min_historical_gaps_for_benchmark and
                median_historical_inter_conversion_gap_minutes > 0 and
                time_diff_minutes > automated_dynamic_threshold and 
                daily_spend_today > min_daily_spend_for_alert):
                triggered_alerts_list.append({
                    "alert_name": "flowInterruption", "vertical_name": vertical_name,
                    "campaign_name": "N/A", "campaign_id": "N/A",
                    "calculated_value": time_diff_minutes, "calculated_value_timeframe": "Minutes since last Click-out/Lead",
                    "benchmark": automated_dynamic_threshold, "benchmark_timeframe": "Automated Dynamic Threshold (min)",
                    "landing_page": "N/A", "spend": daily_spend_today, "leads" : "N/A",
                    "base_threshold": base_threshold
                })

    def check_high_cpa(self, data_hourly_ad_performance_metrics, data_sessions_full_lifecycle, vertical_name, campaign_id, campaign_name, base_threshold, mode, triggered_alerts_list, campaign_name_map, percent_increase=0):
        min_spend_for_cpa_alert = self.config["min_spend_for_cpa_alert"]
        current_spend_6h_df = data_hourly_ad_performance_metrics.filter(
            F.col("timestamp") >= F.expr("current_timestamp() - interval 6 hours")
        ).groupBy("campaign_id").agg(F.sum("spend").alias("current_6h_spend"))
        current_volume_6h_df = data_sessions_full_lifecycle.filter(
            (F.col("conversion_dt") >= F.expr("current_timestamp() - interval 6 hours")) &
            (F.col("conversion_dt").isNotNull())
        ).groupBy("campaign_id").agg(F.count("session_id").alias("current_6h_volume"))
        current_cpa_by_campaign = current_spend_6h_df.join(current_volume_6h_df, "campaign_id", "outer") \
            .withColumn("current_6h_cpa",
                        F.when(F.col("current_6h_volume") == 0, float('inf')).otherwise(F.col("current_6h_spend") / F.col("current_6h_volume")))

        if mode in ["campaign_controlled", "project_controlled"]:
            current_campaign_cpa_row = current_cpa_by_campaign.filter(F.col("campaign_id") == campaign_id).first()
            if current_campaign_cpa_row:
                current_cpa = current_campaign_cpa_row["current_6h_cpa"] or 0
                current_spend = current_campaign_cpa_row["current_6h_spend"]
                current_volume = current_campaign_cpa_row["current_6h_volume"] or 0
            else:
                current_cpa = float('inf')
                current_spend = 0
                current_volume = 0
            effective_threshold = base_threshold
            if percent_increase and percent_increase > 0:
                effective_threshold = base_threshold * (1 + percent_increase / 100)
            if current_cpa > effective_threshold and current_spend > min_spend_for_cpa_alert:
                triggered_alerts_list.append({
                    "alert_name": "highCPA", "vertical_name": vertical_name,
                    "campaign_name": campaign_name, "campaign_id": campaign_id,
                    "calculated_value": current_cpa, "calculated_value_timeframe": "Last 6 Hours",
                    "benchmark": effective_threshold, "benchmark_timeframe": "Controlled Threshold",
                    "landing_page": "N/A", "spend": current_spend, "leads" : current_volume,
                    "base_threshold": base_threshold
                })
        elif mode == "automated_benchmark":
            current_day_of_week = F.dayofweek(F.current_timestamp())
            four_weeks_ago_timestamp = F.current_timestamp() - F.expr("interval 4 weeks")
            spend_history_6h = data_hourly_ad_performance_metrics.filter(
                (F.col("timestamp") >= four_weeks_ago_timestamp) &
                (F.dayofweek(F.col("timestamp")) == current_day_of_week) &
                (F.col("timestamp") < F.current_timestamp())
            ).withColumn(
                "time_window_start", F.date_trunc("hour", F.col("timestamp") - F.expr("interval 6 hours"))
            ).groupBy("campaign_id", "time_window_start").agg(F.sum("spend").alias("window_spend"))
            volume_history_6h = data_sessions_full_lifecycle.filter(
                (F.col("conversion_dt") >= four_weeks_ago_timestamp) &
                (F.dayofweek(F.col("conversion_dt")) == current_day_of_week) &
                (F.col("conversion_dt") < F.current_timestamp()) &
                (F.col("conversion_dt").isNotNull())
            ).withColumn(
                "time_window_start", F.date_trunc("hour", F.col("conversion_dt") - F.expr("interval 6 hours"))
            ).groupBy("campaign_id", "time_window_start").agg(F.count("session_id").alias("window_volume"))
            historical_cpa_per_window = spend_history_6h.join(volume_history_6h, ["campaign_id", "time_window_start"], "outer") \
                .withColumn("cpa",
                            F.when(F.col("window_volume") == 0, float('inf')).otherwise(F.col("window_spend") / F.col("window_volume")))
            window_spec_campaign = Window.partitionBy("campaign_id")
            historical_cpa_with_percentiles = historical_cpa_per_window.withColumn(
                "cpa_10th_percentile", F.percentile_approx(F.col("cpa"), F.lit(0.1), F.lit(1000)).over(window_spec_campaign)
            ).withColumn(
                "cpa_90th_percentile", F.percentile_approx(F.col("cpa"), F.lit(0.9), F.lit(1000)).over(window_spec_campaign)
            )
            filtered_historical_cpa = historical_cpa_with_percentiles.filter(
                (F.col("cpa") >= F.col("cpa_10th_percentile")) &
                (F.col("cpa") <= F.col("cpa_90th_percentile")) &
                (F.col("cpa_10th_percentile").isNotNull()) &
                (F.col("cpa_90th_percentile").isNotNull())
            ).drop("cpa_10th_percentile", "cpa_90th_percentile")
            window_median_ordered = window_spec_campaign.orderBy("cpa").rowsBetween(Window.unboundedPreceding, 0)
            window_total_in_partition = window_spec_campaign.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            df_for_median_mad = filtered_historical_cpa.withColumn(
                "median_cpa_candidate",
                self._weighted_median_candidate_expr("cpa", "window_volume", window_median_ordered, window_total_in_partition)
            )
            campaign_medians = df_for_median_mad.groupBy("campaign_id").agg(F.min("median_cpa_candidate").alias("4wk_dow_weighted_median_cpa"))
            df_with_median_for_mad = filtered_historical_cpa.join(campaign_medians, "campaign_id", "inner")
            df_with_mad_candidate = df_with_median_for_mad.withColumn(
                "mad_cpa_candidate",
                self._weighted_mad_candidate_expr("cpa", "window_volume", "4wk_dow_weighted_median_cpa",
                                            window_spec_campaign.orderBy(F.abs(F.col("cpa") - F.col("4wk_dow_weighted_median_cpa"))).rowsBetween(Window.unboundedPreceding, 0),
                                            window_total_in_partition)
            )
            benchmark_cpa_df = df_with_mad_candidate.groupBy("campaign_id").agg(
                F.first("4wk_dow_weighted_median_cpa").alias("4wk_dow_weighted_median_cpa"),
                F.min("mad_cpa_candidate").alias("4wk_dow_mad_cpa")
            )
            MAD_SCALING_FACTOR = self.config["cpa_mad_scaling_factor"]
            mad_threshold_multiplier = self.config["cpa_mad_threshold_multiplier"]
            benchmark_cpa_df = benchmark_cpa_df.withColumn(
                "high_cpa_threshold",
                F.col("4wk_dow_weighted_median_cpa") + (F.col("4wk_dow_mad_cpa") * MAD_SCALING_FACTOR * mad_threshold_multiplier)
            ).filter(F.col("high_cpa_threshold").isNotNull() & (F.col("high_cpa_threshold") != float('inf')))
            cpa_alert_candidates = current_cpa_by_campaign.join(benchmark_cpa_df, "campaign_id", "inner")
            high_cpa_campaigns = cpa_alert_candidates.filter(
                (F.col("current_6h_cpa") > F.col("high_cpa_threshold")) & 
                (F.col("high_cpa_threshold") != float('inf')) &
                (F.col("current_6h_spend") > min_spend_for_cpa_alert)
            )
            for row in high_cpa_campaigns.collect():
                current_campaign_name = campaign_name_map.get(row['campaign_id'], row['campaign_id'])
                triggered_alerts_list.append({
                    "alert_name": "highCPA", "vertical_name": vertical_name,
                    "campaign_name": current_campaign_name, "campaign_id": row['campaign_id'],
                    "calculated_value": row['current_6h_cpa'], "calculated_value_timeframe": "Last 6 Hours",
                    "benchmark": row['high_cpa_threshold'], "benchmark_timeframe": "Automated Threshold (Weighted Median + MAD)",
                    "landing_page": "N/A", "spend": row['current_6h_spend'], "leads" : row['current_6h_volume'],
                    "base_threshold": base_threshold
                })

    def check_performance_decline(self, data_daily_ad_performance_metrics, data_sessions_full_lifecycle, vertical_name, campaign_id, campaign_name, base_threshold, mode, triggered_alerts_list, campaign_name_map, percent_increase=0):
        min_spend_for_roi_alert = self.config["min_spend_for_roi_alert"]
        todays_revenue_df = data_sessions_full_lifecycle.filter(
            F.to_date("conversion_dt") == F.current_date()
        ).groupBy("campaign_id").agg(F.sum("conversion_payout").alias("todays_revenue"))
        todays_spend_df = data_daily_ad_performance_metrics.filter(
            F.col("date") == F.current_date()
        ).groupBy("campaign_id").agg(F.sum("spend").alias("todays_spend"))
        todays_roi_by_campaign = todays_revenue_df.join(todays_spend_df, "campaign_id", "outer") \
            .withColumn("todays_roi", F.when(F.col("todays_spend") == 0, float('inf')).otherwise(F.col("todays_revenue") / F.col("todays_spend")))

        if mode in ["campaign_controlled", "project_controlled"]:
            current_campaign_roi_row = todays_roi_by_campaign.filter(F.col("campaign_id") == campaign_id).first()
            if current_campaign_roi_row:
                todays_roi = current_campaign_roi_row["todays_roi"]
                todays_spend_volume = current_campaign_roi_row["todays_spend"]
                effective_threshold = base_threshold
                if percent_increase and percent_increase > 0:
                    effective_threshold = base_threshold * (1 + percent_increase / 100)
                if todays_spend_volume > min_spend_for_roi_alert and todays_roi < effective_threshold:
                    triggered_alerts_list.append({
                        "alert_name": "performanceDecline", "vertical_name": vertical_name,
                        "campaign_name": campaign_name, "campaign_id": campaign_id,
                        "calculated_value": todays_roi, "calculated_value_timeframe": "Today",
                        "benchmark": effective_threshold, "benchmark_timeframe": "Controlled Threshold",
                        "landing_page": "N/A", "spend": todays_spend_volume, "leads" : "N/A",
                        "base_threshold": base_threshold
                    })
        elif mode == "automated_benchmark":
            last_week_start_date = F.current_date() - F.expr("interval 7 days")
            last_week_end_date = F.current_date() - F.expr("interval 1 day")
            last_week_revenue_df = data_sessions_full_lifecycle.filter(
                (F.to_date("conversion_dt") >= last_week_start_date) & (F.to_date("conversion_dt") <= last_week_end_date)
            ).groupBy("campaign_id").agg(F.sum("conversion_payout").alias("last_week_revenue"))
            last_week_spend_df = data_daily_ad_performance_metrics.filter(
                (F.col("date") >= last_week_start_date) & (F.col("date") <= last_week_end_date)
            ).groupBy("campaign_id").agg(F.sum("spend").alias("last_week_spend"))
            last_week_roi_by_campaign = last_week_revenue_df.join(last_week_spend_df, "campaign_id", "outer") \
                .withColumn("last_week_roi", F.when(F.col("last_week_spend") == 0, float('inf')).otherwise(F.col("last_week_revenue") / F.col("last_week_spend")))
            benchmark_roi_df = todays_roi_by_campaign.join(last_week_roi_by_campaign, "campaign_id", "inner")
            benchmark_drop_factor = self.config["performance_decline_drop_factor"]
            underperforming_campaigns = benchmark_roi_df.filter(
                (F.col("todays_spend") > min_spend_for_roi_alert) &
                (F.col("last_week_spend") > min_spend_for_roi_alert) &
                (F.col("last_week_roi") != float('inf')) &
                (F.col("todays_roi") < (F.col("last_week_roi") * benchmark_drop_factor))
            )
            for row in underperforming_campaigns.collect():
                current_campaign_name = campaign_name_map.get(row['campaign_id'], row['campaign_id'])
                triggered_alerts_list.append({
                    "alert_name": "performanceDecline", "vertical_name": vertical_name,
                    "campaign_name": current_campaign_name, "campaign_id": row['campaign_id'],
                    "calculated_value": row['todays_roi'], "calculated_value_timeframe": "Today",
                    "benchmark": row['last_week_roi'] * benchmark_drop_factor,
                    "benchmark_timeframe": f"Last Week's Avg ROI * {benchmark_drop_factor}",
                    "landing_page": "N/A", "spend": row['todays_spend'], "leads" : "N/A",
                    "base_threshold": base_threshold
                })

    def check_fb_to_clickout(self, data_hourly_ad_performance_metrics, data_sessions_full_lifecycle, vertical_name, campaign_id, campaign_name, base_threshold, mode, triggered_alerts_list, campaign_name_map, percent_increase=0):
        min_clicks_for_fb_to_clickout = self.config["min_clicks_for_fb_to_clickout"]
        current_fb_clicks_df = data_hourly_ad_performance_metrics.filter(
            F.col("timestamp") >= F.expr("current_timestamp() - interval 24 hours")
        ).groupBy("campaign_id").agg(F.sum("clicks").alias("current_fb_clicks"))
        current_clickouts_df = data_sessions_full_lifecycle.filter(
            (F.col("session_dt") >= F.expr("current_timestamp() - interval 24 hours")) &
            (F.col("postback_id").isNotNull())
        ).groupBy("campaign_id").agg(F.countDistinct("session_id").alias("current_clickouts"))
        current_fb_to_clickout_by_campaign = current_fb_clicks_df.join(current_clickouts_df, "campaign_id", "outer") \
            .withColumn("current_fb_to_clickout_rate",
                        F.when(F.col("current_fb_clicks") == 0, 0).otherwise(F.col("current_clickouts") / F.col("current_fb_clicks")))
        if mode in ["campaign_controlled", "project_controlled"]:
            current_campaign_rate_row = current_fb_to_clickout_by_campaign.filter(F.col("campaign_id") == campaign_id).first()
            if current_campaign_rate_row:
                current_rate = current_campaign_rate_row["current_fb_to_clickout_rate"]
                fb_clicks_volume = current_campaign_rate_row["current_fb_clicks"]
                effective_threshold = base_threshold
                if percent_increase and percent_increase > 0:
                    effective_threshold = base_threshold * (1 + percent_increase / 100)
                if current_rate < effective_threshold and fb_clicks_volume >= min_clicks_for_fb_to_clickout:
                    triggered_alerts_list.append({
                        "alert_name": "fbToClickout", "vertical_name": vertical_name,
                        "campaign_name": campaign_name, "campaign_id": campaign_id,
                        "calculated_value": current_rate, "calculated_value_timeframe": "Last 24 Hours",
                        "benchmark": effective_threshold, "benchmark_timeframe": "Controlled Threshold",
                        "landing_page": "N/A", "spend": "N/A", "leads" : "N/A",
                        "base_threshold": base_threshold
                    })
        elif mode == "automated_benchmark":
            last_week_start_threshold = F.expr("current_timestamp() - interval 8 days")
            last_week_end_threshold = F.expr("current_timestamp() - interval 1 day")
            last_week_fb_clicks_df = data_hourly_ad_performance_metrics.filter(
                (F.col("timestamp") >= last_week_start_threshold) & (F.col("timestamp") < last_week_end_threshold)
            ).groupBy("campaign_id").agg(F.sum("clicks").alias("last_week_fb_clicks"))
            last_week_clickouts_df = data_sessions_full_lifecycle.filter(
                (F.col("session_dt") >= last_week_start_threshold) & (F.col("session_dt") < last_week_end_threshold) &
                (F.col("postback_id").isNotNull())
            ).groupBy("campaign_id").agg(F.countDistinct("session_id").alias("last_week_clickouts"))
            last_week_fb_to_clickout_by_campaign = last_week_fb_clicks_df.join(last_week_clickouts_df, "campaign_id", "outer") \
                .withColumn("last_week_fb_to_clickout_rate",
                            F.when(F.col("last_week_fb_clicks") == 0, 0).otherwise(F.col("last_week_clickouts") / F.col("last_week_fb_clicks")))
            benchmark_fb_to_clickout_df = current_fb_to_clickout_by_campaign.join(last_week_fb_to_clickout_by_campaign, "campaign_id", "inner")
            benchmark_drop_factor = self.config["fb_to_clickout_drop_factor"]
            low_fb_to_clickout_campaigns = benchmark_fb_to_clickout_df.filter(
                (F.col("last_week_fb_to_clickout_rate") > 0) & 
                (F.col("current_fb_to_clickout_rate") < (F.col("last_week_fb_to_clickout_rate") * benchmark_drop_factor)) &
                (F.col("current_fb_clicks") >= min_clicks_for_fb_to_clickout)
            )
            for row in low_fb_to_clickout_campaigns.collect():
                current_campaign_name = campaign_name_map.get(row['campaign_id'], row['campaign_id'])
                triggered_alerts_list.append({
                    "alert_name": "fbToClickout", "vertical_name": vertical_name,
                    "campaign_name": current_campaign_name, "campaign_id": row['campaign_id'],
                    "calculated_value": row['current_fb_to_clickout_rate'], "calculated_value_timeframe": "Last 24 Hours",
                    "benchmark": row['last_week_fb_to_clickout_rate'] * benchmark_drop_factor,
                    "benchmark_timeframe": f"Last Week's Avg Rate * {benchmark_drop_factor}",
                    "landing_page": "N/A", "spend": "N/A", "leads" : "N/A",
                    "base_threshold": base_threshold
                })

    def check_clickout_to_conversion(self, data_sessions_full_lifecycle, vertical_name, campaign_id, campaign_name, base_threshold, mode, triggered_alerts_list, campaign_name_map, percent_increase=0):
        min_clickouts_for_cr_alert = self.config["min_clickouts_for_cr_alert"]
        todays_conversions_df = data_sessions_full_lifecycle.filter(
            (F.to_date("conversion_dt") == F.current_date()) &
            (F.col("conversion_dt").isNotNull()) &
            (F.col("conversion_payout").isNotNull())
        ).groupBy("campaign_id").agg(F.count("session_id").alias("todays_conversions"))
        todays_clickouts_df = data_sessions_full_lifecycle.filter(
            (F.to_date("session_dt") == F.current_date()) &
            (F.col("postback_id").isNotNull())
        ).groupBy("campaign_id").agg(F.count("session_id").alias("todays_clickouts"))
        todays_cr_by_campaign = todays_conversions_df.join(todays_clickouts_df, "campaign_id", "outer") \
            .withColumn("todays_cr", F.when(F.col("todays_clickouts") == 0, 0).otherwise(F.col("todays_conversions") / F.col("todays_clickouts")))
        if mode in ["campaign_controlled", "project_controlled"]:
            current_campaign_cr_row = todays_cr_by_campaign.filter(F.col("campaign_id") == campaign_id).first()
            if current_campaign_cr_row:
                todays_cr = current_campaign_cr_row["todays_cr"]
                todays_clickouts = current_campaign_cr_row["todays_clickouts"]
                effective_threshold = base_threshold
                if percent_increase and percent_increase > 0:
                    effective_threshold = base_threshold * (1 + percent_increase / 100)
                if todays_cr < effective_threshold and todays_clickouts >= min_clickouts_for_cr_alert:
                    triggered_alerts_list.append({
                        "alert_name": "clickoutToConversion", "vertical_name": vertical_name,
                        "campaign_name": campaign_name, "campaign_id": campaign_id,
                        "calculated_value": todays_cr, "calculated_value_timeframe": "Today",
                        "benchmark": effective_threshold, "benchmark_timeframe": "Controlled Threshold",
                        "landing_page": "N/A", "spend": "N/A", "leads" : "N/A",
                        "base_threshold": base_threshold
                    })
        elif mode == "automated_benchmark":
            last_week_conversions_df = data_sessions_full_lifecycle.filter(
                (F.to_date("conversion_dt") == (F.current_date() - F.expr("interval 7 days"))) &
                (F.col("conversion_dt").isNotNull()) &
                (F.col("conversion_payout").isNotNull())
            ).groupBy("campaign_id").agg(F.count("session_id").alias("last_week_conversions"))
            last_week_clickouts_df = data_sessions_full_lifecycle.filter(
                (F.to_date("session_dt") == (F.current_date() - F.expr("interval 7 days"))) &
                (F.col("postback_id").isNotNull())
            ).groupBy("campaign_id").agg(F.count("session_id").alias("last_week_clickouts"))
            last_week_cr_by_campaign = last_week_conversions_df.join(last_week_clickouts_df, "campaign_id", "outer") \
                .withColumn("last_week_cr", F.when(F.col("last_week_clickouts") == 0, 0).otherwise(F.col("last_week_conversions") / F.col("last_week_clickouts")))
            benchmark_cr_df = todays_cr_by_campaign.join(last_week_cr_by_campaign, "campaign_id", "inner")
            benchmark_factor = self.config["clickout_to_conversion_factor"]
            low_cr_campaigns = benchmark_cr_df.filter(
                (F.col("last_week_cr") > 0) &
                (F.col("todays_cr") < (F.col("last_week_cr") * benchmark_factor)) &
                (F.col("todays_clickouts") >= min_clickouts_for_cr_alert)
            )
            for row in low_cr_campaigns.collect():
                current_campaign_name = campaign_name_map.get(row['campaign_id'], row['campaign_id'])
                triggered_alerts_list.append({
                    "alert_name": "clickoutToConversion", "vertical_name": vertical_name,
                    "campaign_name": current_campaign_name, "campaign_id": row['campaign_id'],
                    "calculated_value": row['todays_cr'], "calculated_value_timeframe": "Today",
                    "benchmark": row['last_week_cr'] * benchmark_factor,
                    "benchmark_timeframe": f"Last Week's Avg CR * {benchmark_factor}",
                    "landing_page": "N/A", "spend": "N/A", "leads" : "N/A",
                    "base_threshold": base_threshold
                })

    def check_high_lp_abandonment(self, data_daily_actions_funnel, data_sessions_full_lifecycle, vertical_name, base_threshold, mode, triggered_alerts_list, campaign_name_map, percent_increase=0):
        min_sessions_for_lp_abandonment = self.config["min_sessions_for_lp_abandonment"]
        total_started_by_page = data_sessions_full_lifecycle.filter(
            F.col("session_dt") >= F.expr("current_timestamp() - interval 24 hours")
        ).select("session_id", "page").groupBy("page").agg(
            F.count("session_id").alias("total_started")
        )
        successful_sessions_24h_by_page = data_daily_actions_funnel.filter(
            (F.col("interaction_type") == "loader") & 
            (F.to_date(F.col("date")) >= F.to_date(F.expr("current_timestamp() - interval 24 hours")))
        ).groupBy("page").agg(
            F.sum("users_reached_step").alias("successful_24h_count")
        )
        current_abandonment_rate_by_page = total_started_by_page.join(
            successful_sessions_24h_by_page, on="page", how="left"
        ).withColumn(
            "successful_24h_count", F.coalesce(F.col("successful_24h_count"), F.lit(0))
        ).withColumn(
            "total_abandoned", F.col("total_started") - F.col("successful_24h_count")
        ).withColumn(
            "total_abandoned", F.when(F.col("total_abandoned") < 0, F.lit(0)).otherwise(F.col("total_abandoned"))
        ).withColumn(
            "current_rate", F.when(F.col("total_started") == 0, 0).otherwise(F.col("total_abandoned") / F.col("total_started"))
        )
        if mode == "project_controlled":
            effective_threshold = base_threshold
            if percent_increase and percent_increase > 0:
                effective_threshold = base_threshold * (1 + percent_increase / 100)
            pages_to_alert = current_abandonment_rate_by_page.filter(
                (F.col("current_rate") > effective_threshold) &
                (F.col("total_started") >= min_sessions_for_lp_abandonment)
            )
            for row in pages_to_alert.collect():
                triggered_alerts_list.append({
                    "alert_name": "highLpAbandonment", "vertical_name": vertical_name,
                    "campaign_name": "N/A", "campaign_id": "N/A",
                    "calculated_value": row['current_rate'], "calculated_value_timeframe": "Last 24 Hours",
                    "benchmark": effective_threshold, "benchmark_timeframe": "Controlled Threshold",
                    "landing_page": row['page'], "spend": "N/A", "leads" : "N/A",
                    "base_threshold": base_threshold
                })
        elif mode == "automated_benchmark":
            last_week_start_threshold = F.expr("current_timestamp() - interval 8 days")
            last_week_end_threshold = F.expr("current_timestamp() - interval 1 days")
            total_started_last_week_by_page = data_sessions_full_lifecycle.filter(
                (F.col("session_dt") >= last_week_start_threshold) & (F.col("session_dt") < last_week_end_threshold)
            ).select("session_id", "page").groupBy("page").agg(F.count("session_id").alias("last_week_started"))
            successful_sessions_last_week_by_page = data_daily_actions_funnel.filter(
                (F.col("interaction_type") == "loader") &
                (F.to_date(F.col("date")) >= F.to_date(last_week_start_threshold)) & 
                (F.to_date(F.col("date")) < F.to_date(last_week_end_threshold))
            ).groupBy("page").agg(F.sum("users_reached_step").alias("last_week_successful"))
            page_metrics_df = total_started_last_week_by_page.join(
                successful_sessions_last_week_by_page, on="page", how="left"
            ).withColumn(
                "last_week_successful", F.coalesce(F.col("last_week_successful"), F.lit(0))
            ).withColumn(
                "last_week_abandoned", F.col("last_week_started") - F.col("last_week_successful")
            )
            last_week_rate_df = page_metrics_df.withColumn("last_week_rate", 
                F.when(F.col("last_week_started") == 0, 0).otherwise(F.col("last_week_abandoned") / F.col("last_week_started")))
            benchmark_factor = self.config["lp_abandonment_benchmark_factor"]
            pages_to_alert = current_abandonment_rate_by_page.join(last_week_rate_df, "page", "inner") \
                .filter(
                    (F.col("current_rate") > (F.col("last_week_rate") * benchmark_factor)) &
                    (F.col("total_started") >= min_sessions_for_lp_abandonment) &
                    (F.col("last_week_rate") > 0)
                )
            for row in pages_to_alert.collect():
                triggered_alerts_list.append({
                    "alert_name": "highLpAbandonment", "vertical_name": vertical_name,
                    "campaign_name": "N/A", "campaign_id": "N/A",
                    "calculated_value": row['current_rate'], "calculated_value_timeframe": "Last 24 Hours",
                    "benchmark": row['last_week_rate'] * benchmark_factor,
                    "benchmark_timeframe": f"Last Week's Avg Rate * {benchmark_factor}",
                    "landing_page": row['page'], "spend": "N/A", "leads" : "N/A",
                    "base_threshold": base_threshold
                })

    def run_all_alerts(self):
        """
        Main method to orchestrate the alert checks for all projects.
        This contains the main loop from your original script.
        """
        all_triggered_alerts_flat_list = []
        
        for project in self.projects_data:
            vertical_name = project.get("vertical_name")
            project_triggered_alerts_list = []

            # Dynamically get campaigns for the project
            project_campaigns_info = []
            campaign_name_map = {}

            # Get active campaigns by vertical: either spend>0 OR volume>0 in the past 24 hours
            active_campaigns_by_spend = (
                self.hourly_ad_performance_metrics.filter(
                    (F.col("vertical") == vertical_name) &
                    (F.col("timestamp") >= F.expr("current_timestamp() - interval 24 hours"))
                )
                .groupBy("campaign_id", "campaign_name")
                .agg(F.sum("spend").alias("recent_spend"))
                .filter(F.col("recent_spend") > 0)
            )
            active_campaigns_by_sessions = (
                self.sessions_full_lifecycle.filter(
                    (F.col("vertical") == vertical_name) &
                    (F.col("timestamp") >= F.expr("current_timestamp() - interval 24 hours"))
                )
                .groupBy("campaign_id")
                .agg(F.count("session_id").alias("recent_sessions"))
                .filter(F.col("recent_sessions") > 0)
            )

            active_campaigns_overall = active_campaigns_by_spend.unionByName(active_campaigns_by_sessions).distinct().collect()

            for row in active_campaigns_overall:
                cid = row["campaign_id"]
                cname = row["campaign_name"]
                if cid not in campaign_name_map:
                    project_campaigns_info.append({"campaign_id": cid, "campaign_name": cname if cname else cid})
                    campaign_name_map[cid] = cname if cname else cid
            
            campaign_ids_for_project_data_filter = [c["campaign_id"] for c in project_campaigns_info]

            # Filter relevant Gold tables for the collected campaign_ids
            if campaign_ids_for_project_data_filter:
                project_daily_ad_performance_metrics = self.daily_ad_performance_metrics.filter(
                    F.col("campaign_id").isin(campaign_ids_for_project_data_filter)
                )
                project_hourly_ad_performance_metrics = self.hourly_ad_performance_metrics.filter(
                    F.col("campaign_id").isin(campaign_ids_for_project_data_filter)
                )
                project_sessions_full_lifecycle = self.sessions_full_lifecycle.filter(
                    F.col("campaign_id").isin(campaign_ids_for_project_data_filter)
                )
                project_daily_actions_funnel = self.daily_actions_funnel.filter(
                    F.col("campaign_id").isin(campaign_ids_for_project_data_filter)
                )
            else:
                project_daily_ad_performance_metrics = self.daily_ad_performance_metrics.limit(0)
                project_hourly_ad_performance_metrics = self.hourly_ad_performance_metrics.limit(0)
                project_sessions_full_lifecycle = self.sessions_full_lifecycle.limit(0)
                project_daily_actions_funnel = self.daily_actions_funnel.limit(0)

            all_alerts_config = project.get("alerts", {})

            for alert_name, alert_details in all_alerts_config.items():
                global_base_threshold = alert_details.get("globalBaseThreshold")
                global_percent_increase = alert_details.get("globalPercent", 0)
                is_alert_automated = alert_details.get("automated", False)

                if alert_name in ["highCPA", "performanceDecline", "fbToClickout", "clickoutToConversion"]:
                    for campaign_entry in project_campaigns_info:
                        campaign_id = campaign_entry["campaign_id"]
                        campaign_name = campaign_entry["campaign_name"]
                        
                        current_campaign_hourly_ad_performance_metrics = project_hourly_ad_performance_metrics.filter(F.col("campaign_id") == campaign_id)
                        current_campaign_daily_ad_performance_metrics = project_daily_ad_performance_metrics.filter(F.col("campaign_id") == campaign_id)
                        current_campaign_sessions_full_lifecycle = project_sessions_full_lifecycle.filter(F.col("campaign_id") == campaign_id)

                        if is_alert_automated:
                            mode = "automated_benchmark"
                            base_threshold_to_pass = None
                            percent_increase_to_pass = 0
                        elif global_base_threshold is not None and not is_alert_automated:
                            mode = "controlled_benchmark"
                            base_threshold_to_pass = global_base_threshold
                            percent_increase_to_pass = global_percent_increase
                        else:
                            continue

                        if alert_name == "highCPA":
                            self.check_high_cpa(current_campaign_hourly_ad_performance_metrics, current_campaign_sessions_full_lifecycle, vertical_name, campaign_id, campaign_name, base_threshold_to_pass, mode, project_triggered_alerts_list, campaign_name_map, percent_increase_to_pass)
                        elif alert_name == "performanceDecline":
                            self.check_performance_decline(current_campaign_daily_ad_performance_metrics, current_campaign_sessions_full_lifecycle, vertical_name, campaign_id, campaign_name, base_threshold_to_pass, mode, project_triggered_alerts_list, campaign_name_map, percent_increase_to_pass)
                        elif alert_name == "fbToClickout":
                            self.check_fb_to_clickout(current_campaign_hourly_ad_performance_metrics, current_campaign_sessions_full_lifecycle, vertical_name, campaign_id, campaign_name, base_threshold_to_pass, mode, project_triggered_alerts_list, campaign_name_map, percent_increase_to_pass)
                        elif alert_name == "clickoutToConversion":
                            self.check_clickout_to_conversion(current_campaign_sessions_full_lifecycle, vertical_name, campaign_id, campaign_name, base_threshold_to_pass, mode, project_triggered_alerts_list, campaign_name_map, percent_increase_to_pass)

                elif alert_name in ["volumeDrop", "flowInterruption", "highLpAbandonment"]:
                    data_smm_for_project_alerts = project_hourly_ad_performance_metrics
                    data_sessions_for_project_alerts = project_sessions_full_lifecycle
                    data_actions_for_project_alerts = project_daily_actions_funnel

                    if global_base_threshold is not None and is_alert_automated:
                        mode = "project_controlled"
                        base_threshold_to_pass = global_base_threshold
                        percent_increase_to_pass = global_percent_increase
                        
                        if alert_name == "volumeDrop":
                            self.check_volume_drop(data_smm_for_project_alerts, data_sessions_for_project_alerts, vertical_name, base_threshold_to_pass, mode, project_triggered_alerts_list, campaign_name_map, percent_increase_to_pass)
                        elif alert_name == "flowInterruption":
                            self.check_flow_interruption(data_smm_for_project_alerts, data_sessions_for_project_alerts, vertical_name, base_threshold_to_pass, mode, project_triggered_alerts_list, campaign_name_map, percent_increase_to_pass)
                        elif alert_name == "highLpAbandonment":
                            self.check_high_lp_abandonment(data_actions_for_project_alerts, data_sessions_for_project_alerts, vertical_name, base_threshold_to_pass, mode, project_triggered_alerts_list, campaign_name_map, percent_increase_to_pass)
                    elif is_alert_automated:
                        mode = "automated_benchmark"
                        base_threshold_to_pass = None
                        percent_increase_to_pass = 0
                        if alert_name == "volumeDrop":
                            self.check_volume_drop(data_smm_for_project_alerts, data_sessions_for_project_alerts, vertical_name, base_threshold_to_pass, mode, project_triggered_alerts_list, campaign_name_map, percent_increase_to_pass)
                        elif alert_name == "flowInterruption":
                            self.check_flow_interruption(data_smm_for_project_alerts, data_sessions_for_project_alerts, vertical_name, base_threshold_to_pass, mode, project_triggered_alerts_list, campaign_name_map, percent_increase_to_pass)
                        elif alert_name == "highLpAbandonment":
                            self.check_high_lp_abandonment(data_actions_for_project_alerts, data_sessions_for_project_alerts, vertical_name, base_threshold_to_pass, mode, project_triggered_alerts_list, campaign_name_map, percent_increase_to_pass)

            if project_triggered_alerts_list:
                all_triggered_alerts_flat_list.extend(project_triggered_alerts_list)
                print(f"\n--- ALERTS TRIGGERED FOR VERTICAL {vertical_name} ---")
                for alert_data in project_triggered_alerts_list:
                    print(f"  Alert Type: {alert_data.get('alert_name', 'N/A')}")
                    print(f"  vertical: {alert_data.get('vertical_name', 'N/A')} ({alert_data.get('vertical_name', 'N/A')})")
                    print(f"  Campaign: {alert_data.get('campaign_name', 'N/A')} ({alert_data.get('campaign_id', 'N/A')})")
                    print(f"  Calculated Value: {alert_data.get('calculated_value')}, Timeframe: {alert_data.get('calculated_value_timeframe')}")
                    print(f"  Benchmark: {alert_data.get('benchmark')}, Timeframe: {alert_data.get('benchmark_timeframe')}")
                    print(f"  Landing Page: {alert_data.get('landing_page', 'N/A')}, Spend: {alert_data.get('spend', 'N/A')}")
                print("------------------------------------------")

                for alert_object in project_triggered_alerts_list:
                    payload_to_send = alert_object
                    try:
                        res = requests.post(
                            "https://api.internal-monitoring.com/v1/alerts/publish",
                            headers={"Content-Type": "application/json", "x-api-key": static_token},
                            data=json.dumps(payload_to_send)
                        )
                        if res.status_code == 200:
                            print(f"  Sent alert '{alert_object.get('alert_name', 'Unknown')}' for Vertical: {vertical_name}")
                        else:
                            print(f"  Failed to send alert '{alert_object.get('alert_name', 'Unknown')}' for Vertical: {vertical_name}: {res.status_code} - {res.text}")
                    except Exception as e:
                        print(f"  Error sending alert '{alert_object.get('alert_name', 'Unknown')}' for Vertical: {vertical_name}: {str(e)}")
            else:
                print(f"INFO: No alerts triggered for vertical {vertical_name} during this run.")
            print(f"--- Finished processing Vertical: {vertical_name} ---")

        print("\n===== All Projects Processed. Summary of Triggered Alerts =====")
        if all_triggered_alerts_flat_list:
            print(json.dumps(all_triggered_alerts_flat_list, indent=2))
        else:
            print("No alerts triggered for any project.")
        print("===============================================================")
        print("--- Script Finished ---")

if __name__ == "__main__":
    # Assumes 'spark' is a globally available SparkSession object in the Databricks environment
    alerts_manager = AlertsManager(spark_session=spark)
    alerts_manager.run_all_alerts()
