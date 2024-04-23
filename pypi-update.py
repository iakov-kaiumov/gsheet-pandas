import os
import dotenv

dotenv.load_dotenv('.env')

username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')

os.system('poetry build')
os.system(f'poetry publish --username {username} --password {password}')
