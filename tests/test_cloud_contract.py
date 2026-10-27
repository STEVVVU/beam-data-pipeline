"""Local SDK contract test; no Google service is contacted."""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import pipeline


def test_bigquery_sink_wiring(tmp_path, monkeypatch):
    calls = []

    class Capture(beam.PTransform):
        def expand(self, rows):
            return rows | beam.Map(lambda row: row)

    def sink(table, **kwargs):
        calls.append((table, kwargs))
        return Capture()

    monkeypatch.setattr(beam.io, "WriteToBigQuery", sink)
    pipeline.run(["--source", "products", "--input", "fixtures/products.jsonl", "--output", str(tmp_path / "out"), "--bq-table", "test:dataset.products_Dataflow"])
    assert calls[0][0] == "test:dataset.products_Dataflow"
    assert calls[0][1]["schema"] == pipeline.SCHEMAS["products"]
    assert calls[0][1]["write_disposition"] == beam.io.BigQueryDisposition.WRITE_EMPTY
    options = PipelineOptions(["--runner=DataflowRunner", "--project=test", "--region=us-central1", "--temp_location=gs://test/tmp", "--setup_file=setup.py", "--requirements_file=requirements-cloud-lock.txt"])
    assert options.get_all_options()["runner"] == "DataflowRunner"
