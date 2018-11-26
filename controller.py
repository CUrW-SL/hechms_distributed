import json
from datetime import datetime, timedelta
from flask import Flask, request, send_from_directory, jsonify
from flask_negotiate import consumes, produces
from flask_json import FlaskJSON, JsonError, json_response
from flask_uploads import UploadSet, configure_uploads
from os import path
from config import UPLOADS_DEFAULT_DEST, HECHMS_LIBS_DIR, DISTRIBUTED_MODEL_TEMPLATE_DIR, INIT_DATE_TIME_FORMAT
from input.shape_util.polygon_util import get_sub_ratios, get_timeseris, get_sub_catchment_rain_files
from input.gage.model_gage import create_gage_file
from input.control.model_control import create_control_file
from input.run.model_run import create_run_file
from config import BACK_DAYS

app = Flask(__name__)
flask_json = FlaskJSON()

# Flask-Uploads configs
app.config['UPLOADS_DEFAULT_DEST'] = path.join(UPLOADS_DEFAULT_DEST, 'FLO2D')
app.config['UPLOADED_FILES_ALLOW'] = ['csv', 'run', 'control']

# upload set creation
model_distributed = UploadSet('configfiles', extensions=('csv','run','control'))

configure_uploads(app, model_distributed)
flask_json.init_app(app)


@app.route('/')
def hello_world():
    return 'Welcome to HecHms(Distributed) Server!'


@app.route('/HECHMS/distributed/init-run', methods=['POST'])
def init_run():
    req_args = request.args.to_dict()
    # Check whether run-name is specified and valid.
    if 'run-name' not in req_args.keys() or not req_args['run-name']:
        raise JsonError(status_=400, description='run-name is not specified.')
    run_name = req_args['run-name']
    if not is_valid_run_name(run_name):
        raise JsonError(status_=400, description='run-name cannot contain spaces or colons.')
    # Valid base-dt must be specified at the initialization phase
    if 'base-dt' not in req_args.keys() or not req_args['base-dt']:
        raise JsonError(status_=400, description='base-dt is not specified.')
    base_dt = req_args['base-dt']
    if not is_valid_init_dt(base_dt):
        raise JsonError(status_=400, description='Given base-dt is not in the correct format: %s'
                                                 % INIT_DATE_TIME_FORMAT)
    # Valid run-dt must be specified at the initialization phase
    if 'run-dt' not in req_args.keys() or not req_args['run-dt']:
        raise JsonError(status_=400, description='run-dt is not specified.')
    run_dt = req_args['run-dt']
    if not is_valid_init_dt(run_dt):
        raise JsonError(status_=400, description='Given run-dt is not in the correct format: %s'
                                                 % INIT_DATE_TIME_FORMAT)

    today = datetime.today().strftime('%Y-%m-%d')
    input_dir_rel_path = path.join(today, run_name, 'input')
    # Check whether the given run-name is already taken for today.
    input_dir_abs_path = path.join(UPLOADS_DEFAULT_DEST, 'HECHMS', 'distributed', input_dir_rel_path)
    if path.exists(input_dir_abs_path):
        raise JsonError(status_=400, description='run-name: %s is already taken for today: %s.' % (run_name, today))

    req_files = request.files
    if 'inflow' in req_files and 'outflow' in req_files and 'raincell' in req_files:
        model_distributed.save(req_files['rainfall'], folder=input_dir_rel_path, name='daily_rain.csv')
        model_distributed.save(req_files['model_run'], folder=input_dir_rel_path, name='model.run')
        model_distributed.save(req_files['model_control'], folder=input_dir_rel_path, name='model.control')
        model_distributed.save(req_files['model_gage'], folder=input_dir_rel_path, name='model.gage')
    else:
        raise JsonError(status_=400, description='Missing required input files. Required inflow, outflow, raincell.')

    # Save run configurations.
    #prepare_flo2d_run_config(input_dir_abs_path, run_name, base_dt, run_dt)

    run_id = 'FLO2D:model250m:%s:%s' % (today, run_name)  # TODO save run_id in a DB with the status
    return json_response(status_=200, run_id=run_id, description='Successfully saved files.')


@app.route('/HECHMS/distributed/start-run', methods=['GET', 'POST'])
def start_run():
    req_args = request.args.to_dict()


@app.route('/HECHMS/distributed/get-output/output.zip', methods=['GET', 'POST'])
def get_output():
    req_args = request.args.to_dict()


@app.route('/HECHMS/distributed/extract/water-level', methods=['POST'])
@consumes('application/json')
def extract_data():
    req_args = request.args.to_dict()


@app.route('/HECHMS/distributed/ratio', methods=['GET', 'POST'])
def get_sub_catchment_ratios():
    print('get_sub_catchment_ratios.')
    return jsonify({'sub_catchment_ratios': get_sub_ratios()})


@app.route('/HECHMS/distributed/timeseries', methods=['GET', 'POST'])
def get_sub_catchment_timeseries():
    print('get_sub_catchment_timeseries.')
    return jsonify({'timeseries': get_timeseris()})


@app.route('/HECHMS/distributed/rain-fall', methods=['GET', 'POST'])
@app.route('/HECHMS/distributed/rain-fall/<string:run_datetime>',  methods=['GET', 'POST'])
@app.route('/HECHMS/distributed/rain-fall/<string:run_datetime>/<int:back_days>',  methods=['GET', 'POST'])
def get_sub_catchment_rain_fall(run_datetime=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), back_days=2):
    print('get_sub_catchment_rain_fall.')
    print('run_datetime : ', run_datetime)
    print('back_days : ', back_days)
    run_datetime = datetime.strptime(run_datetime, '%Y-%m-%d %H:%M:%S')

    end_date = run_datetime.strftime('%Y-%m-%d %H:%M:%S')
    start_date = run_datetime - timedelta(days=back_days)
    start_date = start_date.strftime('%Y-%m-%d %H:%M:%S')

    file_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    file_name = 'DailyRain-{}.csv'.format(file_date.strftime('%Y-%m-%d'))
    print('file_name : ', file_name)
    print('{start_date, end_date} : ', {start_date, end_date})
    get_sub_catchment_rain_files(file_name, start_date, end_date)
    return jsonify({'timeseries': {}})


@app.route('/HECHMS/distributed/create-gage-file', methods=['GET', 'POST'])
def get_gage_file():
    print('create_gage_file.')
    create_gage_file('distributed_model', '2018-10-08 10:00:00', '2018-10-11 10:00:00')
    return jsonify({'timeseries': {}})

@app.route('/HECHMS/distributed/create-control-file', methods=['GET', 'POST'])
def get_control_file():
    print('create_gage_file.')
    create_control_file('distributed_model', '2018-10-07 10:00:00', '2018-10-10 10:00:00')
    return jsonify({'timeseries': {}})


@app.route('/HECHMS/distributed/create-run-file', methods=['GET', 'POST'])
def get_run_file():
    print('create_run_file.')
    create_run_file('distributed_model', '2018-10-11 11:00:00')
    return jsonify({'timeseries': {}})


def is_valid_run_name(run_name):
    """
    Checks the validity of the run_name. run_name cannot have spaces or colons.
    :param run_name: <class str> provided run_name.
    :return: <bool> True if valid False if not.
    """
    return run_name and not (' ' in run_name or ':' in run_name)


def is_valid_init_dt(date_time):
    """
    Checks the validity of given date_time. Given date_time should be of "yyyy-mm-dd_HH:MM:SS"
    :param date_time: datetime instance
    :return: boolean, True if valid False otherwise
    """
    try:
        datetime.strptime(date_time, INIT_DATE_TIME_FORMAT)
        return True
    except ValueError:
        return False


if __name__ == '__main__':
    app.run()
