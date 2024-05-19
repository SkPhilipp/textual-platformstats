import os
import zipfile

from tools.common.logs import log


def unpack(support_bundle_path):
    if not support_bundle_path.endswith('.zip'):
        raise Exception('Expected a .zip file')
    file = zipfile.ZipFile(support_bundle_path, 'r')
    filename = os.path.basename(support_bundle_path)
    filename_without_ext = os.path.splitext(filename)[0]
    filename_without_ext_short = filename_without_ext[:15]
    dir_name = os.path.dirname(support_bundle_path)
    dir_name = os.path.abspath(os.path.join(dir_name, filename_without_ext_short))
    log.info(f'Unpacking "{support_bundle_path}" to {dir_name}')
    file.extractall(path=dir_name)
    file.close()

    log.info(f'Unpacking nested zip files in {dir_name}')
    for root, dirs, files in os.walk(dir_name):
        for file in files:
            if file.endswith('.zip'):
                file_path = os.path.join(root, file)
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(root)

    log.info('Locating relevant log files')
    match = ['router-request.log']
    log_files = []
    for root, dirs, files in os.walk(dir_name):
        for file in files:
            if any(m in file for m in match):
                log_files.append(os.path.join(root, file))
                log.info(f'Found "{os.path.relpath(os.path.join(root, file), start=dir_name)}"')

    if len(log_files) == 0:
        raise Exception('No log files found')

    return log_files
