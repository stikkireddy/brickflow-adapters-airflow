black-check:
	@pip install "black>=22.8.0, <23.0.0"
	@black --check brickflow_airflow_110

fmt:
	@pip install "black>=22.8.0, <23.0.0"
	@black brickflow_airflow_110

dev:
	@poetry install --all-extras
	@pip install cdktf
	@pip install "black>=22.8.0, <23.0.0"
	@pip install "prospector>=1.7.7, <2.0.0"


check: black-check
	@pip install "prospector>=1.7.7, <2.0.0"
	@prospector --profile prospector.yaml