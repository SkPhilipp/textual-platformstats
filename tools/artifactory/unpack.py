import os
import zipfile


def unpack(support_bundle_path):
    if not support_bundle_path.endswith('.zip'):
        raise Exception('Expected a .zip file')
    file = zipfile.ZipFile(support_bundle_path, 'r')
    filename = os.path.basename(support_bundle_path)
    filename_without_ext = os.path.splitext(filename)[0]
    filename_without_ext_short = filename_without_ext[:15]
    dir_name = os.path.dirname(support_bundle_path)
    dir_name = os.path.abspath(os.path.join(dir_name, filename_without_ext_short))
    print(f'Unpacking {support_bundle_path} to {dir_name}')
    file.extractall(path=dir_name)
    file.close()

    print('Unzipping nested zip files:')
    for root, dirs, files in os.walk(dir_name):
        for file in files:
            if file.endswith('.zip'):
                file_path = os.path.join(root, file)
                print(f'- {os.path.relpath(file_path, start=dir_name)}')
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(root)

    print('Seeking log files:')
    log_files = []
    for root, dirs, files in os.walk(dir_name):
        for file in files:
            if file.endswith('.log'):
                log_files.append(os.path.join(root, file))
                print(f'- {os.path.relpath(os.path.join(root, file), start=dir_name)}')

    # verify that router-request.log is present
    if not any('router-request.log' in log_file for log_file in log_files):
        raise Exception('Expected "router-request.log" in the unpacked support bundle tree.')

    return log_files
