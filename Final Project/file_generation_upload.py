import boto3
import os
import csv
import random

s3_client = boto3.client(
    's3',
    aws_access_key_id='x',
    aws_secret_access_key='x'
)

# Declare lists that values should be taken from
firstname = ['John', 'Dana', 'Scott', 'Marc', 'Steven', 'Michael', 'Albert', 'Johanna', 'David', 'Paul', 'Josh', 'Don', 'Dakota', 'Kris', 'Georgina', 'Elena', 'Carl']
secondname = ['Wang', 'Vuitton', 'Gabanna', 'Kors', 'Jacobs', 'Stileto', 'Armani', 'Boss', 'Hermes', 'Prada', 'Chanel', 'Dior', 'Balenciaga', 'Burberry', 'Laurent']
city = ['NewYork', 'Haifa', 'Munchen', 'London', 'PaloAlto',  'TelAviv', 'Kiel', 'Hamburg', 'Miami', 'Paris', 'Madried', 'Istanbul', 'Casablanca', 'Vienna', 'Prague'] 

def file_generator(file_type, number_of_files):

    if file_type == 'small':
        directory_name = 'smallFiles1'
        max_rows = 4600 # about 1 KB of data
    elif file_type == 'large':
        directory_name = 'largeFiles1'
        max_rows = 4618000 #about 10 MB of data

    for file_number in range(1, number_of_files+1): 

        # assemble file name according to format
        file_name = f'{file_type}File[{file_number}].csv'

        # assemble file path 
        file_path = os.path.join(directory_name, file_name)

        with open(file_path, 'w') as file:

            writer = csv.writer(file)
            writer.writerow(["firstname", "secondname", "city"])

            for row in range(0, max_rows):
                row = [random.choice(firstname), random.choice(secondname), random.choice(city)]
                writer.writerow(row)

    file.close()

    print(f'{number_of_files} files has been created and stored in the {directory_name} directory.')

def upload_to_s3(file_type):

    # Choose dir name based on the type of file it is
    if file_type == 'small':
        directory_name = 'smallFiles1'
    elif file_type == 'large':
        directory_name = 'largeFiles1'
    
    # Generate path to local files
    path = os.path.join('/Users/agatha.benichou/Desktop', directory_name)
    files = os.listdir(path)

    # For every file, assemble local file path and key to store them to S3
    for file in files:
        local_file_name = os.path.join(path, file)
        key = f'{directory_name}/{file}'
        print(key)

        # Upload the file to correct directory in S3
        try:
            s3_client.upload_file(Filename=local_file_name, Bucket='bigdata-final-proj-gal-aggie', Key=key)
        except FileNotFoundError:
            print("The file was not found")

    print(f'Finished uploading {len(files)} files to the {directory_name} directory in bigdata-final-proj-gal-aggie bucket')

def cleanup():
    if os.path.isdir('/Users/agatha.benichou/Desktop/largeFiles'):
        path = '/Users/agatha.benichou/Desktop/largeFiles'
        for filename in os.listdir(path):
            try:
                os.remove(path+'/'+filename)
            except:
                print('')
    os.rmdir('largeFiles')
    print(f'Deleted largeFiles/')

    if os.path.isdir('/Users/agatha.benichou/Desktop/smallFiles/'):
        path = '/Users/agatha.benichou/Desktop/smallFiles'
        for filename in os.listdir(path):
            try:
                os.remove(path+'/'+filename)
            except:
                print('')
    os.rmdir('smallFiles/')
    print(f'Deleted smallFiles/')


if __name__ == "__main__":

    # Create directories
    try:
        os.mkdir('smallFiles1') 
        os.mkdir('largeFiles') 
    except:
        print('Directories have already been created')

    # Generate small and large files
    file_generator('small', 1000) # 1000 files of 1KB = 100MB
    file_generator('large', 1) # 1 file of 100MB = 100MB

    # Upload all the files to S3
    upload_to_s3('small')
    upload_to_s3('large')

    # Cleanup and delete local directories
    cleanup()