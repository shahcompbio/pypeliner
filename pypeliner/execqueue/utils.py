import os


def log_text(debug_filenames):
    text = ''
    for debug_type, debug_filename in debug_filenames.items():
        preamble = '-' * 10 + ' ' + debug_type + ' ' + '-' * 10
        if not os.path.exists(debug_filename):
            text += preamble + ' (missing)\n'
            continue
        else:
            text += preamble + '\n'
        with open(debug_filename, 'r') as debug_file:
            text += debug_file.read()
    return text


def qsub_format_name(name):
    return name.strip('/').rstrip('/').replace('/', '.').replace(':', '_')
