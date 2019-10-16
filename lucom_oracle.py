"""
This script compares grades from Data from Warehouse to API to see if Student Data in the LMS is inconsistent.
"""

from os import system, environ
from requests import Session
import cx_Oracle
import getpass

system('cls')

print('Canvas Data vs API Response'.center(85))

# Credentials are stored in environment variables
USERNAME = input("Username: ") #environ.get('DBUSERNAME')
PASSWORD = getpass.getpass()                      # environ.get('DBPW')
DATABASE = environ.get('DATABASE')

TOKEN = environ.get('TOKEN') #Token stored in environmental variables
HEADER = {'Authorization': 'Bearer ' + TOKEN}
COURSE_URL = environ.get('LUCOM_DOMAIN') + 'api/v1/users/{}/{}'
ACCOUNT_URL = environ.get('LUCOM_DOMAIN') + 'api/v1/accounts/{}/enrollments/{}'
PERPAGE = 100


""" ORACLE SQL to return student info such as name, course, term and grade """
query = """
			SELECT DISTINCT ed.canvas_id, etd.name, cd.name, cd.canvas_id,  ud.name, ud.canvas_id, csf.current_score

			FROM utl_p_canvas.enrollment_dim    ed 

			JOIN utl_p_canvas.user_dim    ud 
			ON   ud.id = ed.user_id
			AND  ud.instance = ed.instance

			JOIN utl_p_canvas.course_dim    cd 
			ON   cd.id = ed.course_id
			AND  cd.instance = ed.instance
			AND  cd.sis_source_id IS NOT NULL  

			JOIN utl_p_canvas.enrollment_term_dim etd
			ON   etd.id = cd.enrollment_term_id
			AND  etd.instance = cd.instance                    

			JOIN utl_p_canvas.course_score_fact csf
			ON   csf.enrollment_id = ed.id
			AND  csf.instance = ed.instance

			INNER JOIN utl_p_canvas.pseudonym_dim pd
			      ON ud.id = pd.user_id
			      AND ud.instance = pd.instance
			                 
			INNER JOIN general.gobtpac            gobtpac
			   ON gobtpac.gobtpac_external_user = LOWER(substr(pd.unique_name, 1,(case when LOWER(pd.unique_name) like '%@liberty%' then (instr(pd.unique_name, '@') - 1)else length(pd.unique_name)end)))

			inner JOIN  saturn.spriden spri
			    ON spri.spriden_pidm = gobtpac.gobtpac_pidm
			    AND spri.spriden_change_ind is NULL
			    
			WHERE ed.instance = 'LUCAN'
			AND   ed.type = 'StudentEnrollment'
			AND   ed.workflow_state = 'completed'

			ORDER BY 1		
		"""

def db_connect():
    """
	This function connects to Oracle Database and executes Query above
	"""
    con = cx_Oracle.connect(USERNAME, PASSWORD, DATABASE)
    cursor = con.cursor()
    sql = query
    cursor.execute(str(sql))
    results = cursor.fetchall()
    return results

def api_connect(edid):
    """
	This function uses the enrollment id from the query to return current score from API
	This will be compared to Canvas Data for differences
	"""

    with Session() as sesh:
        url = ACCOUNT_URL.format(str(1), str(edid))
        data = {'per_page': PERPAGE, 'recursive' : 1}
        request = sesh.get(url, headers=HEADER, data=data)
        response = request.json()
        return response

if __name__ == "__main__":

	ENROLLMENT_LIST = db_connect()

	print('EVALUATING GRADES....')
	for i in ENROLLMENT_LIST:
	    ed_id = i[0]

	    api_grade = api_connect(ed_id)[u'grades'][u'current_score']
	    canvas_data_grade = i[-1]

	    if canvas_data_grade == api_grade:
	        print('grades match for', i[4], ' - ', i[2])
	    else:
	        print('***** GRADES DO NOT MATCH FOR', i[4], ' - ', i[2])
