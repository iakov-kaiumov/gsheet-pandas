import os
import dotenv

dotenv.load_dotenv('.env')

username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')

os.system('python setup.py sdist')
os.system('python setup.py bdist_wheel --universal ')
os.system(f'twine upload --skip-existing dist/* -u {username} -p {password}')
