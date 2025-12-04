# 🚦 Air Compressors Anomaly Detector

## 📑 Project Description
A project from previous work experiences.

Detect air compressors' potential malfunction via DBSCAN.

## ☁️ Clustering-Based Detector
### 📈 Available Features

Each air compressor only has minutely runtime series as the only feature,
making it hard for supervised learning. Thus, clustering is chosen as the method.

### 🛠️ Selected Clustering Algorithm: DBSCAN
**https://scikit-learn.org/stable/modules/generated/sklearn.cluster.DBSCAN.html**

DBSCAN is very great at identifying outliers as well as minority data points.
Minority data points imply abnormality, as do outliers which are even more severe.

## ⛓️ Detector Pipeline
1. Read each air compressor's hourly runtime series from database.
2. Calculate last 7 days' average hourly runtime series.
3. Do DBSCAN on the differential of hourly runtime - last 7 days' average hourly runtime.
4. Identify the hours when minority clusters and outliers happen. Only consider positive differentials, implying runtime spikes.
5. From the hours in step 4, find out these hours' respective runtime level.
6. If runtime level from step 5 hits threshold, mark these hours as abnormalities, as runtime spikes combined with above-threshold runtime = red flag 🚩.
7. Store these abnormal hours in database as potential malfunction alerts.
8. 