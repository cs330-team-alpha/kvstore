init:
	pip install -r requirements.txt

test:
	venv/bin/py.test tests/test_kv.py