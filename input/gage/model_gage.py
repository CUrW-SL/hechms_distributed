from config import GAGE_MANAGER_TEMPLATE, GAGE_TEMPLATE, GAGE_FILE_NAME
from input.shape_util.polygon_util import get_valid_gages
from datetime import datetime


def create_gage_file(model_name, start_datetime, end_datetime):
    gage_file = GAGE_FILE_NAME.replace('{MODEL_NAME}', model_name)
    valid_gages = get_valid_gages(start_datetime, end_datetime)
    print('valid_gage_names : ', valid_gages.keys())
    gage_manager_template = GAGE_MANAGER_TEMPLATE
    gage_manager_template = gage_manager_template.replace('{MODEL_NAME}', model_name)
    gage_template = GAGE_TEMPLATE

    start_date = datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end_datetime, '%Y-%m-%d %H:%M:%S')
    start_date = start_date.strftime('%d %b %Y, %H:%M')
    end_date = end_date.strftime('%d %b %Y, %H:%M')
    with open(gage_file, 'w') as file:
        file.write(gage_manager_template)
        file.write('\n\n')
        gage_template = gage_template.replace('{MODEL_NAME}', model_name)
        gage_template = gage_template.replace('{START_DATE}', start_date)
        gage_template = gage_template.replace('{END_DATE}', end_date)
        for key in valid_gages.keys():
            modified_gage_template = gage_template.replace('{GAGE_NAME}', key)
            file.write(modified_gage_template)
            file.write('\n\n')
    file.close()





