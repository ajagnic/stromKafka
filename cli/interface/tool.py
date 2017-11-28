""" CLI tool for communications b/w client and API. """
import click
import requests
import json
import os
import datetime

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

url = 'http://127.0.0.1:5000'

def _print_ver(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.secho(__version__, fg='yellow')
    ctx.exit()

def _abort_if_false(ctx, param, value):
    if not value:
        ctx.abort()

def _convert_to_utc(date_string):
    """ Expected input: YYYY-MM-DD-HH:MM:SS """
    big_time_tmp = date_string.split("-")
    year = int(big_time_tmp[0])
    month = int(big_time_tmp[1])
    day = int(big_time_tmp[2])
    time_arr = big_time_tmp[3].split(":")
    hours = int(time_arr[0])
    minutes = int(time_arr[1])
    seconds = int(time_arr[2])
    dt = datetime.datetime(year, month, day, hours, minutes, seconds)
    return dt.timestamp()

@click.group()
@click.option('--version', '--v', 'version', is_flag=True, callback=_print_ver, expose_value=False, is_eager=True, help="Current version")
def dstream():
    """ Entry-point. Parent command for all DStream methods. """
    pass

@click.command()
def welcome():
    """ Usage instructions for first time users. """
    click.secho("\nJust Do It.\n", fg='magenta', bold=True)
    click.pause()

@click.command()
@click.argument('path')
def locate(path):
    """ Tool for opening specified files in file explorer. """
    click.launch(path, locate=True)

@click.command()
@click.option('-template', '-t', 'template', prompt=True, type=click.File('r'), help="Template file with required and custom fields")
@click.option('--yes', is_flag=True, callback=_abort_if_false, expose_value=False, prompt="\nInitialize new DStream with this template?", help="Bypass confirmation prompt")
def define(template):
    """ Upload template file for DStream. """
    template_data = template.read()
    click.secho("\nSending template file...", fg='white')
    #Try send template to server, if success...collect stream_token
    try:
        ret = requests.post(url + "/api/define", data={'template':template_data})
    except:
        click.secho("\nConnection Refused!...\n", fg='red', reverse=True)
    else:
        click.secho(str(ret.status_code), fg='yellow')
        click.secho(ret.text, fg='yellow')
        if ret.status_code == 202:
            token = ret.text
        else:
            click.secho("\nServer Error!...\n", fg='red', reverse=True)
        #Try load template as json and set stream_token field, if success...store tokenized template in new file
        try:
            json_template = json.loads(template_data)
            json_template['stream_token'] = token
            template_filename = os.path.basename(template.name) # NOTE: TEMP, REFACTOR OUT OF TRY
            path_list = template_filename.split('.')
            template_name = path_list[0]
            template_ext = path_list[1] # NOTE: TEMP, FILE UPLOAD EXTENSION
            print("Found File Extension: .{}".format(template_ext))  # NOTE: TEMP
        except:
            click.secho("\nProblem parsing template file!...\n", fg='red', reverse=True)
        else:
            click.secho("\nTemplate has been tokenized with...{}".format(json_template['stream_token']), fg='white')
            template_file = open("{}_token.txt".format(template_name), "w")
            template_file.write(json.dumps(json_template))
            template_file.close()
            click.secho("New template stored locally as '{}_token.txt'.\n".format(template_name))

@click.command()
@click.option('-source', '-s', 'source', prompt=True, type=click.Choice(['kafka', 'file']), help="Specify source of data")
@click.option('--kafka-topic', default=None, help="If source is kafka, specify topic")
@click.option('-token', '-tk', 'token', prompt=True, type=click.File('r'), help="Tokenized template file for verification")
def add_source(source, kafka_topic, token):
    """ Declare source of data: file upload or kafka stream. """
    #Check if topic was supplied when source is kafka
    if source == 'kafka' and kafka_topic == None:
        click.secho("No topic specified, please re-run command.", fg='yellow', reverse=True)
    else:
        cert = token.read()
        #Try loading template as json and retrieving token, if success...pass
        try:
            json_cert = json.loads(cert)
            tk = json_cert['stream_token']
        except:
            click.secho("\nThere was an error parsing that file and/or the token was not found!...\n", fg='yellow', reverse=True)
        else:
            click.secho("\nFound stream_token: " + tk, fg='white')
            click.secho("\nSending source for this DStream...\n", fg='white')
            #Try posting data to server, if success...return status_code
            try:
                ret = requests.post(url + "/api/add-source", data={'source':source, 'topic':kafka_topic, 'token':tk})
            except:
                click.secho("\nConnection Refused!...\n", fg='red', reverse=True)
            else:
                click.secho(str(ret.status_code), fg='yellow')
                click.secho(ret.text + '\n', fg='yellow')

@click.command()
@click.option('-filepath', '-f', 'filepath', prompt=True, type=click.Path(exists=True), help="File-path of data file to upload")
@click.option('-token', '-tk', 'token', prompt=True, type=click.File('r'), help="Tokenized template file for verification")
def load(filepath, token):
    """ Provide file-path of data to upload, along with tokenized_template for this DStream. """
    click.secho("\nTokenizing data fields of {}".format(click.format_filename(filepath)), fg='white')
    cert = token.read()
    #Try load client files as json, if success...pass
    try:
        json_data = json.load(open(filepath))
        json_cert = json.loads(cert)
    except:
        click.secho("There was an error accessing/parsing those files!...\n", fg='red', reverse=True)
    else:
        #Try collect stream_token, if success...pass
        try:
            tk = json_cert['stream_token']
            if tk is None:
                raise ValueError
        except:
            click.secho("Token not found in provided template!...\n", fg='yellow', reverse=True)
        else:
            click.secho("Found stream_token: " + tk + '\n', fg='white')
            #Try set stream_token fields to collected token, if success...pass
            try:
                with click.progressbar(json_data) as bar:
                    for obj in bar:
                        obj['stream_token'] = tk
            except:
                click.secho("Data file not correctly formatted!...\n", fg='red', reverse=True)
            else:
                click.secho("\nSending data...", fg='white')
                #Try send data with token to server, if success...return status_code
                try:
                    ret = requests.post(url + "/api/load", data={'data':json.dumps(json_data)})
                except:
                    click.secho("Connection Refused!...\n", fg='red', reverse=True)
                else:
                    click.secho(str(ret.status_code), fg='yellow')
                    click.secho(ret.text + '\n', fg='yellow')

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
@click.command()
@click.option('-datetime', '-d', 'time', type=str, multiple=True, help="Datetime to collect from (YYYY-MM-DD-HH:MM:SS)")
@click.option('-utc', type=str, multiple=True, help="UTC-formatted time to collect from")
@click.option('--all', '--a', 'a', is_flag=True, is_eager=True, help="Collect all data")
@click.option('-token', '-tk', 'tk', prompt=True, type=click.File('r'), help="Tokenized template file for verification")
def raw(time, utc, a, tk):
    """
    \b
     Collect all raw data for specified datetime or time-range*.
     *Options can be supplied twice to indicate a range.
    """
    cert = tk.read()
    try:
        json_cert = json.loads(cert)
    except:
        click.secho("There was an error accessing/parsing those files!...\n", fg='red', reverse=True)
    else:
        try:
            token = json_cert['stream_token']
            if token is None:
                raise ValueError
        except:
            click.secho("Token not found in provided template!...\n", fg='yellow', reverse=True)
        else:
            click.secho("Found stream_token: " + token + '\n', fg='white')
    if a:
        try:
            ret = requests.get(url + "/api/get/raw?range=ALL&token={}".format(token))
        except:
            click.secho("Connection Refused!...", fg='red', reverse=True)
        else:
            click.secho(str(ret.status_code), fg='yellow')
            click.secho(ret.text, fg='yellow')
    elif utc:
        if len(utc) == 1:
            try:
                ret = requests.get(url + "/api/get/raw?time={}&token={}".format(utc[0], token))
            except:
                click.secho("Connection Refused!...", fg='red', reverse=True)
            else:
                click.secho(str(ret.status_code), fg='yellow')
                click.secho(ret.text, fg='yellow')
        elif len(utc) == 2:
            try:
                ret = requests.get(url + "/api/get/raw?range={}&token={}".format(utc, token))
            except:
                click.secho("Connection Refused!...", fg='red', reverse=True)
            else:
                click.secho(str(ret.status_code), fg='yellow')
                click.secho(ret.text, fg='yellow')
        else:
            click.secho("Too many arguments given!({})...".format(len(utc)), fg='yellow', reverse=True)
    elif time:
        if len(time) == 1:
            utime = _convert_to_utc(time[0])
            try:
                ret = requests.get(url + "/api/get/raw?time={}&token={}".format(utime, token))
            except:
                click.secho("Connection Refused!...", fg='red', reverse=True)
            else:
                click.secho(str(ret.status_code), fg='yellow')
                click.secho(ret.text, fg='yellow')
        elif len(time) == 2:
            utime_zero = _convert_to_utc(time[0])
            utime_one = _convert_to_utc(time[1])
            utime = [utime_zero, utime_one]
            try:
                ret = requests.get(url + "/api/get/raw?range={}&token={}".format(utime, token))
            except:
                click.secho("Connection Refused!...", fg='red', reverse=True)
            else:
                click.secho(str(ret.status_code), fg='yellow')
                click.secho(ret.text, fg='yellow')
        else:
            click.secho("Too many arguments given!({})...".format(len(time)), fg='yellow', reverse=True)
    else:
        click.secho("No options given, try '--all'...", fg='white')

@click.command()
@click.option('-datetime', '-d', 'time', type=str, multiple=True, help="Datetime to collect from (YYYY-MM-DD-HH:MM:SS)")
@click.option('-utc', type=str, multiple=True, help="UTC-formatted time to collect from")
@click.option('--all', '--a', 'a', is_flag=True, is_eager=True, help="Collect all data")
@click.option('-token', '-tk', 'tk', prompt=True, type=click.File('r'), help="Tokenized template file for verification")
def filtered(time, utc, a, tk):
    """
    \b
     Collect all filtered data for specified datetime or time-range*.
     *Options can be supplied twice to indicate a range.
    """
    cert = tk.read()
    try:
        json_cert = json.loads(cert)
    except:
        click.secho("There was an error accessing/parsing those files!...\n", fg='red', reverse=True)
    else:
        try:
            token = json_cert['stream_token']
            if token is None:
                raise ValueError
        except:
            click.secho("Token not found in provided template!...\n", fg='yellow', reverse=True)
        else:
            click.secho("Found stream_token: " + token + '\n', fg='white')
    if a:
        try:
            ret = requests.get(url + "/api/get/filtered?range=ALL&token={}".format(token))
        except:
            click.secho("Connection Refused!...", fg='red', reverse=True)
        else:
            click.secho(str(ret.status_code), fg='yellow')
            click.secho(ret.text, fg='yellow')
    elif utc:
        if len(utc) == 1:
            try:
                ret = requests.get(url + "/api/get/filtered?time={}&token={}".format(utc[0], token))
            except:
                click.secho("Connection Refused!...", fg='red', reverse=True)
            else:
                click.secho(str(ret.status_code), fg='yellow')
                click.secho(ret.text, fg='yellow')
        elif len(utc) == 2:
            try:
                ret = requests.get(url + "/api/get/filtered?range={}&token={}".format(utc, token))
            except:
                click.secho("Connection Refused!...", fg='red', reverse=True)
            else:
                click.secho(str(ret.status_code), fg='yellow')
                click.secho(ret.text, fg='yellow')
        else:
            click.secho("Too many arguments given!({})...".format(len(utc)), fg='yellow', reverse=True)
    elif time:
        if len(time) == 1:
            utime = _convert_to_utc(time[0])
            try:
                ret = requests.get(url + "/api/get/filtered?time={}&token={}".format(utime, token))
            except:
                click.secho("Connection Refused!...", fg='red', reverse=True)
            else:
                click.secho(str(ret.status_code), fg='yellow')
                click.secho(ret.text, fg='yellow')
        elif len(time) == 2:
            utime_zero = _convert_to_utc(time[0])
            utime_one = _convert_to_utc(time[1])
            utime = [utime_zero, utime_one]
            try:
                ret = requests.get(url + "/api/get/filtered?range={}&token={}".format(utime, token))
            except:
                click.secho("Connection Refused!...", fg='red', reverse=True)
            else:
                click.secho(str(ret.status_code), fg='yellow')
                click.secho(ret.text, fg='yellow')
        else:
            click.secho("Too many arguments given!({})...".format(len(time)), fg='yellow', reverse=True)
    else:
        click.secho("No options given, try '--all'...", fg='white')


@click.command()
@click.option('-datetime', '-d', 'time', type=str, multiple=True, help="Datetime to collect from (YYYY-MM-DD-HH:MM:SS)")
@click.option('-utc', type=str, multiple=True, help="UTC-formatted time to collect from")
@click.option('--all', '--a', 'a', is_flag=True, is_eager=True, help="Collect all data")
@click.option('-token', '-tk', 'tk', prompt=True, type=click.File('r'), help="Tokenized template file for verification")
def derived_params(time, utc, a, tk):
    """
    \b
     Collect all derived parameters for specified datetime or time-range*.
     *Options can be supplied twice to indicate a range.
    """
    cert = tk.read()
    try:
        json_cert = json.loads(cert)
    except:
        click.secho("There was an error accessing/parsing those files!...\n", fg='red', reverse=True)
    else:
        try:
            token = json_cert['stream_token']
            if token is None:
                raise ValueError
        except:
            click.secho("Token not found in provided template!...\n", fg='yellow', reverse=True)
        else:
            click.secho("Found stream_token: " + token + '\n', fg='white')
    if a:
        try:
            ret = requests.get(url + "/api/get/derived_params?range=ALL&token={}".format(token))
        except:
            click.secho("Connection Refused!...", fg='red', reverse=True)
        else:
            click.secho(str(ret.status_code), fg='yellow')
            click.secho(ret.text, fg='yellow')
    elif utc:
        if len(utc) == 1:
            try:
                ret = requests.get(url + "/api/get/derived_params?time={}&token={}".format(utc[0], token))
            except:
                click.secho("Connection Refused!...", fg='red', reverse=True)
            else:
                click.secho(str(ret.status_code), fg='yellow')
                click.secho(ret.text, fg='yellow')
        elif len(utc) == 2:
            try:
                ret = requests.get(url + "/api/get/derived_params?range={}&token={}".format(utc, token))
            except:
                click.secho("Connection Refused!...", fg='red', reverse=True)
            else:
                click.secho(str(ret.status_code), fg='yellow')
                click.secho(ret.text, fg='yellow')
        else:
            click.secho("Too many arguments given!({})...".format(len(utc)), fg='yellow', reverse=True)
    elif time:
        if len(time) == 1:
            utime = _convert_to_utc(time[0])
            try:
                ret = requests.get(url + "/api/get/derived_params?time={}&token={}".format(utime, token))
            except:
                click.secho("Connection Refused!...", fg='red', reverse=True)
            else:
                click.secho(str(ret.status_code), fg='yellow')
                click.secho(ret.text, fg='yellow')
        elif len(time) == 2:
            utime_zero = _convert_to_utc(time[0])
            utime_one = _convert_to_utc(time[1])
            utime = [utime_zero, utime_one]
            try:
                ret = requests.get(url + "/api/get/derived_params?range={}&token={}".format(utime, token))
            except:
                click.secho("Connection Refused!...", fg='red', reverse=True)
            else:
                click.secho(str(ret.status_code), fg='yellow')
                click.secho(ret.text, fg='yellow')
        else:
            click.secho("Too many arguments given!({})...".format(len(time)), fg='yellow', reverse=True)
    else:
        click.secho("No options given, try '--all'...", fg='white')


@click.command()
@click.option('-datetime', '-d', 'time', type=str, multiple=True, help="Datetime to collect from (YYYY-MM-DD-HH:MM:SS)")
@click.option('-utc', type=str, multiple=True, help="UTC-formatted time to collect from")
@click.option('--all', '--a', 'a', is_flag=True, is_eager=True, help="Collect all data")
@click.option('-token', '-tk', 'tk', prompt=True, type=click.File('r'), help="Tokenized template file for verification")
def events(time, utc, a, tk):
    """
    \b
     Collect all event data for specified datetime or time-range*.
     *Options can be supplied twice to indicate a range.
    """
    cert = tk.read()
    try:
        json_cert = json.loads(cert)
    except:
        click.secho("There was an error accessing/parsing those files!...\n", fg='red', reverse=True)
    else:
        try:
            token = json_cert['stream_token']
            if token is None:
                raise ValueError
        except:
            click.secho("Token not found in provided template!...\n", fg='yellow', reverse=True)
        else:
            click.secho("Found stream_token: " + token + '\n', fg='white')
    if a:
        try:
            ret = requests.get(url + "/api/get/events?range=ALL&token={}".format(token))
        except:
            click.secho("Connection Refused!...", fg='red', reverse=True)
        else:
            click.secho(str(ret.status_code), fg='yellow')
            click.secho(ret.text, fg='yellow')
    elif utc:
        if len(utc) == 1:
            try:
                ret = requests.get(url + "/api/get/events?time={}&token={}".format(utc[0], token))
            except:
                click.secho("Connection Refused!...", fg='red', reverse=True)
            else:
                click.secho(str(ret.status_code), fg='yellow')
                click.secho(ret.text, fg='yellow')
        elif len(utc) == 2:
            try:
                ret = requests.get(url + "/api/get/events?range={}&token={}".format(utc, token))
            except:
                click.secho("Connection Refused!...", fg='red', reverse=True)
            else:
                click.secho(str(ret.status_code), fg='yellow')
                click.secho(ret.text, fg='yellow')
        else:
            click.secho("Too many arguments given!({})...".format(len(utc)), fg='yellow', reverse=True)
    elif time:
        if len(time) == 1:
            utime = _convert_to_utc(time[0])
            try:
                ret = requests.get(url + "/api/get/events?time={}&token={}".format(utime, token))
            except:
                click.secho("Connection Refused!...", fg='red', reverse=True)
            else:
                click.secho(str(ret.status_code), fg='yellow')
                click.secho(ret.text, fg='yellow')
        elif len(time) == 2:
            utime_zero = _convert_to_utc(time[0])
            utime_one = _convert_to_utc(time[1])
            utime = [utime_zero, utime_one]
            try:
                ret = requests.get(url + "/api/get/events?range={}&token={}".format(utime, token))
            except:
                click.secho("Connection Refused!...", fg='red', reverse=True)
            else:
                click.secho(str(ret.status_code), fg='yellow')
                click.secho(ret.text, fg='yellow')
        else:
            click.secho("Too many arguments given!({})...".format(len(time)), fg='yellow', reverse=True)
    else:
        click.secho("No options given, try '--all'...", fg='white')

# d-stream group
dstream.add_command(locate)
dstream.add_command(welcome)
dstream.add_command(define)
dstream.add_command(add_source)
dstream.add_command(load)
#
dstream.add_command(raw)
dstream.add_command(filtered)
dstream.add_command(derived_params)
dstream.add_command(events)