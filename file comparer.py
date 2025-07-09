import re


def extract_function_names(file_path, language):
    """
    Extracts function names from a file based on the programming language.
    """
    with open(file_path, 'r') as file:
        content = file.read()

    if language == 'python':
        # Match Python function definitions
        return re.findall(r'def\s+(\w+)\s*\(', content)
    elif language == 'r':
        # Match R function definitions
        return re.findall(r'(\w+)\s*<-\s*function', content)
    else:
        raise ValueError("Unsupported language. Use 'python' or 'r'.")


# Paths to the files
python_file = (r'D:\Backup\R language\mark1-flexdashboard-report-main\mark1-flexdashboard-report-main\flexdashboard'
               r'\utilities.py')
r_file = r'D:\Backup\R language\mark1-flexdashboard-report-main\mark1-flexdashboard-report-main\Utilities.R'

# Extract function names
python_functions = set(extract_function_names(python_file, 'python'))
r_functions = set(extract_function_names(r_file, 'r'))

# Compare functions
missing_in_python = r_functions - python_functions
missing_in_r = python_functions - r_functions

print("Functions missing in Python:", missing_in_python)
print("Functions missing in R:", missing_in_r)