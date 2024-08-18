from pandas import json_normalize
import logging

def process_user(ti):
    try:
        user_data = ti.xcom_pull(task_ids="extract_user")
        if not user_data or 'results' not in user_data or not user_data['results']:
            raise ValueError("Invalid or empty user data")

        user = user_data['results'][0]
        processed_user = json_normalize({
            'firstname': user['name']['first'],
            'lastname': user['name']['last'],
            'country': user['location']['country'],
            'username': user['login']['username'],
            'password': user['login']['password'],
            'email': user['email']
        })
        
        output_path = '/tmp/processed_user.csv'
        processed_user.to_csv(output_path, index=None, header=False)
        logging.info(f"User data processed and saved to {output_path}")
        return output_path
    except Exception as e:
        logging.error(f"Error processing user data: {str(e)}")
        raise