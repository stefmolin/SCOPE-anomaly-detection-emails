import argparse
import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from urllib.parse import urlsplit, urlunsplit, urlencode
import subprocess
import time
import yaml
from jinja2 import Environment, PackageLoader
import logging
import os
import collections
import base64
import uuid
import datetime
from operator import attrgetter

# Logging configuration
FORMAT = '[%(levelname)s] [ %(name)s ] %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)
logger = logging.getLogger(os.path.basename(__file__))

class EmailImage:
    def __init__(self, filename):
        self.filename = filename
        self.uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, filename)) + str(uuid.uuid1())

class Email:
    def __init__(self, ts, standard_images, tableau_links, base_feedback_url, base_sherlock_url, template_name):
        # Prepare email body
        env = Environment(loader=PackageLoader('templates', ''))
        template = env.get_template(template_name)
        self.body = template.render(ts=ts, standard_images=standard_images,
                                    tableau_links=tableau_links,
                                    base_feedback_url=base_feedback_url,
                                    base_sherlock_url=base_sherlock_url,
                                    build_metis_feedback_urls=build_metis_feedback_urls,
                                    build_sherlock_link=build_sherlock_link)
        self.MIME_msg = self.build_MIME_message(ts, standard_images)

    def build_MIME_message(self, ts, standard_images):
        message = MIMEMultipart()
        email_body = self.body
        mime_text = MIMEText(self.body, 'html')
        message.attach(mime_text)
        for partner in ts.partners:
            for alert, graph_list in partner.alerts.items():
                for graph in graph_list:
                    message = self.attach_MIME_images(graph, message)
        for img in list(standard_images.values()):
            message = self.attach_MIME_images(img, message)
        return message

    def attach_MIME_images(self, img, msg):
        with open(img.filename, 'rb') as image:
            mime_image = MIMEImage(image.read())
        mime_image.add_header('Content-ID', '<{cid}>'.format(cid=img.uuid))
        msg.attach(mime_image)
        return(msg)

class TS:
    def __init__(self, name, email,
                 manager_name, manager_email,
                 cost_center, country, subregion,
                 region, ranking, run_date, partners):
        self.name = name
        self.email = email
        self.user_name = email.split('@')[0]
        self.manager_name = manager_name
        self.manager_email = manager_email
        self.cost_center = cost_center
        self.country = country
        self.subregion = subregion
        self.region = region
        self.ranking = ranking
        self.run_date = run_date
        self.partners = sorted(partners, key=attrgetter('name'))

class Partner:
    def __init__(self, partner_name, partner_id, alerts):
        self.name = partner_name
        self.id = partner_id
        self.alerts = alerts
    @property
    def ordered_alerts(self):
        return collections.OrderedDict(sorted(self.alerts.items()))

class Graph(EmailImage):
    def __init__(self, filename, metadata=None):
        super().__init__(filename)
        # specify data that will be needed for feedback links or HTML but aren't an alert type themselves
        # i.e { 'event_name' : 'listing' } and can use graph.metadata.get('event_name')
        # which will return None if that doesn't exist meaning it doesn't apply and doesn't need to be
        # in that section of the email
        self.metadata = metadata

class AlertParser:
    def __init__(self, dict_from_yaml):
        # this class should parse the alert dictionary to make the objects
        self.ts = self.parse_TS(dict_from_yaml)

    def parse_TS(self, data):
        return [ TS(ts['name'], ts['email'], ts['manager_name'],
                    ts['manager_email'], ts['cost_center'],
                    ts['country'], ts['subregion'], ts['region'],
                    ts['ranking'], ts['run_date'],
                    self.parse_partners(ts['partners']))
                for ts in data['TS']]

    def parse_partners(self, data):
        return [ Partner(partner['partner_name'], partner['partner_id'],
                         self.parse_alerts(partner))
                 for partner in data ]

    def parse_alerts(self, data):
        alerts = {}
        for alert in data['alerts']:
            site_type = alert.get('site')
            if site_type:
                alerts['site_events:' + site_type] = [ Graph(event['graph'],
                                                       metadata={ 'event_name' : event['event_name'] })
                                                       for event in alert['event'] ]
            else:
                pass
        return alerts

class HistoryHelper:
    def __init__(self, run_history_file, email_history_file, geo):
        self.run_history_file = run_history_file
        self.email_history_file = email_history_file
        self.geo = geo

    def script_ran_today(self):
        log_text = "Checking if detection script ran already for {g} today via file {h}:"
        log_text = log_text.format(g=self.geo, h=self.run_history_file)
        logger.debug(log_text)
        open(self.run_history_file, "a").close() # Creates the file if it does not exist
        with open(self.run_history_file, 'r') as yaml_file:
            history = yaml.load(yaml_file)
        history = history if isinstance(history, list) else []
        logger.debug(history)
        date = str(datetime.date.today()) + " " + self.geo
        if date in history:
            return True
        else:
            return False

    def check_email_sent(self, to_address, subject):
        log_text = "Checking if email was sent already today to {to} with subject {s} via file {h}"
        log_text = log_text.format(to=to_address, s=subject, h=self.email_history_file)
        logger.debug(log_text)
        open(self.email_history_file, "a").close() # Creates the file if it does not exist
        with open(self.email_history_file, 'r') as yaml_file:
            history = yaml.load(yaml_file)
        history = history if isinstance(history, dict) else {}
        logger.debug(history)
        date = str(datetime.date.today())
        if (date in history
                and to_address in history[date]
                and subject in history[date][to_address]):
            return True
        else:
            return False

    def log_email_sent(self, to_address, subject):
        log_text = "Log that email was sent already today via file {h}:"
        log_text = log_text.format(h=self.email_history_file)
        logger.debug(log_text)
        open(self.email_history_file, "a").close() # Creates the file if it does not exist
        with open(self.email_history_file, 'r') as yaml_file:
            history = yaml.load(yaml_file)
        history = history if isinstance(history, dict) else {}
        logger.debug(history)
        date = str(datetime.date.today())
        if not date in history: history[date] = { to_address: [subject] }
        elif not to_address in history[date]: history[date][to_address] = [subject]
        elif not subject in history[date][to_address]: history[date][to_address].append(subject)
        with open(self.email_history_file, 'w') as yaml_file:
            yaml.dump(history, yaml_file, default_flow_style=False)

    def log_script_ran_today(self):
        log_text = "Log that detection script ran already for {g} today via file {h}:"
        log_text = log_text.format(g=self.geo,h=self.run_history_file)
        logger.debug(log_text)
        open(self.run_history_file, "a").close() # Creates the file if it does not exist
        with open(self.run_history_file, 'r') as yaml_file:
            history = yaml.load(yaml_file)
        history = history if isinstance(history, list) else []
        logger.debug(history)
        date = str(datetime.date.today()) + " " + self.geo
        if not date in history: history.append(date)
        with open(self.run_history_file, 'w') as yaml_file:
            yaml.dump(history, yaml_file, default_flow_style=False)

class EmailClient:
    def __init__(self, host, sender, password):
        self.host = host
        self.sender = sender
        self.password = password

    def send_email(self, MIME_message, subject, to, cc=None, bcc=None):
        MIME_message['From'] = self.sender
        MIME_message['To'] = to
        MIME_message['Subject'] = subject
        if bcc:
            MIME_message['Bcc'] = bcc
        if cc:
            MIME_message['Cc'] = cc
        attempts = 0
        max_attempts = 3
        while attempts < max_attempts:
            try:
                with smtplib.SMTP(self.host) as server:
                    server.ehlo()
                    server.starttls()
                    server.login(self.sender, self.password)
                    server.send_message(MIME_message)
                logger.info("Email sent")
                break
            except Exception as e:
                attempts += 1
                if attempts < max_attempts:
                    time.sleep(1)
                else:
                    logger.info("Email sending failed with exception {e}".format(e=e))

def build_metis_feedback_urls(base_url, ts, partner, site_type, event_name):
    split_url = urlsplit(base_url)
    query_params = { 'series' : '{partner_name} {event_name} on {site_type}'
                                .format(partner_name=partner.name,
                                        event_name=event_name,
                                        site_type=site_type),
                     'partner_name' : partner.name,
                     'partner_id' : partner.id,
                     'site_type' : site_type,
                     'event_name' : event_name,
                     'kpi' : 'tag_events',
                     'run_date' : ts.run_date,
                     'username' : ts.user_name,
                     'cost_center' : ts.cost_center,
                     'ranking' : ts.ranking,
                     'country' : ts.country,
                     'subregion' : ts.subregion,
                     'region' : ts.region }

    feedback_button_values = [True, False]
    query_strings = {}
    for button in feedback_button_values:
        query_params.update({ 'is_alert' : button })
        query_strings[button] = urlencode(query_params)

    feedback_url_dict = { feedback_button : urlunsplit((split_url.scheme,
                                                        split_url.netloc,
                                                        split_url.path,
                                              	        query_strings[feedback_button],
                                                        ''))
                          for feedback_button in feedback_button_values }

    return feedback_url_dict

def build_sherlock_link(base_url, partner):
    split_url = urlsplit(base_url)

    partner_id = partner.id
    end_date = datetime.date.today() - datetime.timedelta(1)
    start_date = end_date - datetime.timedelta(15)

    query_params = {'partner_id' : partner_id,
                    'start_date' : str(start_date),
                    'end_date' : str(end_date),
                    'kpi' : 'site_events'}
    query_string = urlencode(query_params)

    sherlock_url = urlunsplit((split_url.scheme, split_url.netloc, split_url.path, query_string, ''))
    return sherlock_url

if __name__ == '__main__':
    # Parse the command line arguments
    parser = argparse.ArgumentParser(description='Send alerts to TS')
    parser.add_argument('--last-try')
    parser.add_argument('--config-file')
    args = parser.parse_args()
    global_config_file = "config.yml"
    global_config = yaml.load(open(global_config_file))
    config = yaml.load(open(args.config_file))
    pass_config = yaml.load(open(global_config['email']['config_pass']))
    password = base64.standard_b64decode(pass_config['password']).decode('ascii')

    history = HistoryHelper(run_history_file=global_config['run_history']['py_history_file'],
                            email_history_file=global_config['email_history']['py_ts_email_sent_history'],
                            geo=config['ts']['geo'])

    if history.script_ran_today():
        logger.info("Script already succesfully ran today. Exiting...")
    else:
        # Fire R detection script
        command = "Rscript --vanilla ts_detect.r {config_file} last_try={is_last_try}"
        command = command.format(config_file=args.config_file, is_last_try=args.last_try)
        completed_process = subprocess.run(command, shell=True)
        returncode = completed_process.returncode
        logger.debug("Return code: {r}".format(r=returncode))
        if returncode != 0:
            logger.info("Detection script was not able to complete. Exiting...")
        else:
            # Parse the data from the detection script
            source_data = yaml.load(open(config['results_file']))
            # If YAML is empty, it means no alerts were triggered; log success and exit
            if not source_data:
              logger.info("No alerts were detected. Exiting...")
            else:
                logger.debug(source_data)

                feedback_link = global_config['feedback_link']['base']
                tableau_links = global_config['tableau_links']
                sherlock_link = global_config['sherlock_link']['base']

                standard_email_images = { 'scope_logo' : 'scope_header.png',
                                          'feedback_button_true' : 'scope_true_L.png',
                                          'feedback_button_false' : 'scope_false_L.png',
                                          'company_logo' : 'scope_footer.png',
                                          'sherlock_button' : 'scope_sherlock_L.png' }
                standard_email_images = { key : EmailImage(os.path.join('images', img))
                                          for key, img in standard_email_images.items() }

                email_client = EmailClient(host=global_config['email']['host'],
                                           sender=global_config['email']['sender'],
                                           password=password)

                ts_list = AlertParser(source_data).ts
                for ts in ts_list:
                    if os.environ.get('DEPLOYMENT_ENVIRONMENT') == 'production':
                        toaddr = ts.email
                        subject_prefix = ""
                    else:
                        toaddr = ', '.join([global_config['test_mode']['email']])
                        subject_prefix = "[TEST] "
                    subject = subject_prefix + config['ts']['subject'].format(username=ts.name.title())
                    bcc = ', '.join(global_config['maintainers'])
                    log_text = "Sending email to {to} with subject {subject}"
                    logger.info(log_text.format(to=toaddr, subject=subject))
                    email = Email(ts=ts, standard_images=standard_email_images,
                                tableau_links=tableau_links,
                                base_feedback_url=feedback_link,
                                base_sherlock_url=sherlock_link,
                                template_name=config['ts']['template'])
                    email_client.send_email(email.MIME_msg, subject, toaddr, bcc=bcc)
                    history.log_email_sent(to_address=toaddr, subject=subject)
                logger.info("Script has finished. Exiting...")
            history.log_script_ran_today()
