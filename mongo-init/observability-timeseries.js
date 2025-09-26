// Initializes Observability scalar/summary time-series collections used by the mimic.
(function () {
  const dbName = "Observability";
  const metricDb = db.getSiblingDB(dbName);

  function ensureTimeseriesCollection(name) {
    const collections = new Set(metricDb.getCollectionNames());
    if (!collections.has(name)) {
      metricDb.createCollection(name, {
        timeseries: {
          timeField: "timestamp",
          metaField: "tags",
          granularity: "seconds",
        },
      });
    }

    const existingIndexes = metricDb[name].getIndexes().map((idx) => idx.name);
    if (!existingIndexes.includes("metric_timestamp")) {
      metricDb[name].createIndex({ metricName: 1, timestamp: 1 }, { name: "metric_timestamp" });
    }
  }

  ["scalar", "summary"].forEach(ensureTimeseriesCollection);
})();
