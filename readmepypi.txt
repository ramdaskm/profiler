change setup.py with new version
python3 setup.py build bdist_wheel
#pip install twine
twine upload dist/dbr_profiler_tool-x.x.x-py3-none-any.whl 

------
https://pypi.org/help/#file-name-reuse