# Automated Performance Alerting System (PySpark)

## 📌 Project Overview
This system provides real-time monitoring and anomaly detection for high-scale ad-tech data. It was designed to replace manual monitoring and reduce the "Time to Detection" for performance drops and technical tracking issues.

## 🚀 Key Features
* **Statistical Anomaly Detection:** Utilizes **Weighted Median** and **Median Absolute Deviation (MAD)** to identify CPA outliers while accounting for volume variance.
* **Automated Benchmarking:** Compares current performance against a rolling 7-day historical average to handle weekly seasonality.
* **Multi-Channel Monitoring:** Tracks Volume Drops, Flow Interruptions, High CPA, and ROI Decline across various verticals.

## 🛠 Tech Stack
* **Language:** Python
* **Data Processing:** PySpark (Spark SQL)
* **Integration:** REST APIs (JSON payloads)
* **Architecture:** Object-Oriented Programming (OOP)

## 📈 Business Impact
* **Proactive Detection:** Identified tracking interruptions 4 hours faster than previous manual checks.
* **Accuracy:** Reduced false-positive alerts by ~20% by implementing the MAD scaling factor instead of static thresholds.
