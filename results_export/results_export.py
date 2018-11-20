import re
import MySQLdb
import yaml
import sshtunnel

with open('config.yaml', 'r') as yamlconfig:
    config = yaml.load(yamlconfig)

rescalescore = re.compile(r'^\d{1,2}\.\d$')
reclasscode = re.compile(r'^(?P<campus>\w+)\/(?P<year>\d{2})(?P<subject>[^\d]+)\w+$')

student_box = {}
student_counts = {}

results = []
results_scale = []
results_nine = []
results_nine_scale = []

class Student(object):

    def __init__(self, id, campus, year, name, house):
        self.id = id
        self.campus = campus
        self.year = int(year)
        self.name = name
        self.house = house
        self.results = []

    def __len__(self):
        return len(self.results)

    @property
    def scale_avg_bysubject(self):
        avgs = dict()

        for subject, results in self.scale_results_bysubject.items():
            if len(results) is 0:
                avgs[subject] = 0
            else:
                avgs[subject] = sum(results) / len(results)
        return avgs

    @property
    def scale_counts_bysubject(self):
        scale_counts = {}
        for subject, results in self.scale_results_bysubject.items():
            scale_counts[subject] = len(results)
        return scale_counts

    @property
    def scale_results_bysubject(self):
        scale_results = dict()
        for result in [x for x in self.results if x.scale is not None]:
            if result.subject not in scale_results:
                scale_results[result.subject] = []
            scale_results[result.subject].append(result.scale)
        return scale_results

    @property
    def year_level_bykla(self):
        year_levels = {'E': '', 'M': '', 'S': '', 'H': '', 'T': '', 'L': '', 'R': '', 'A': '', 'P': ''}
        for result in self.results:
            year_levels[result.kla] = result.year
        return year_levels

    @property
    def scale_results_bykla(self):
        scale_results = dict()
        for result in [x for x in self.results if x.scale is not None]:
            kla = result.kla
            if result.year is 9 and result.campus == 'CE':
                if result.subject[0] in ['A', 'T', 'L']:
                    kla = result.kla
                else:
                    kla = result.subject
            if kla not in scale_results:
                scale_results[kla] = []
            scale_results[kla].append(result.scale)
        return scale_results
    
    @property
    def norm_results_bykla(self):
        norm_results = dict()
        for result in [x for x in self.results if x.scale is None and x.normalized is not None]:
            if result.kla not in norm_results:
                norm_results[result.kla] = []
            norm_results[result.kla].append(result.normalized)
        return norm_results
    
    @property
    def scale_results(self):
        return [x.scale for x in self.results if x.scale is not None]

    @property
    def norm_results(self):
        return None

    @property
    def norm_counts_bykla(self):
        counts = {}
        for kla, results in self.norm_results_bykla.items():
            counts[kla] = len(results)
        return counts

    @property
    def scale_counts_bykla(self):
        counts = {}
        for kla, results in self.scale_results_bykla.items():
            counts[kla] = len(results)
        return counts

    @property
    def scale_avg_bykla(self):
        avgs = dict()

        for kla, results in self.scale_results_bykla.items():
            if len(results) is 0:
                avgs[kla] = 0
            else:
                avgs[kla] = sum(results) / len(results)
        return avgs

    @property
    def scale_avg(self):
        results = self.scale_results
        if len(results) is 0:
            return 0
        return sum(results) / len(results)

    @property
    def norm_avg_bykla(self):
        norm_avgs = dict()
        for kla, results in self.norm_results_bykla.items():
            if len(results) is 0:
                norm_avgs[kla] = 0
            else:
                norm_avgs[kla] = sum(results) / len(results)
        return norm_avgs

    @property
    def avg_norm(self):
        results = [x.normalized for x in self.results if x.scale is None and x.normalized is not None]
        if len(results) is 0:
            return 0
        return sum(results) / len(results)


    def add_result(self, result):
        self.results.append(result)


class Result(object):

    def __init__(self, id, box, campus_class_code, mark, normalized, name):
        self.id = id
        self.box = box
        self.name = name
        self.class_code = campus_class_code[3:]
        self.normalized = float(normalized) if normalized else None
        self.mark = mark
        self.absent = False
        self.submitted = True
        self.campus = None
        self.year = None
        self.subject = None
        self.scale = None
        self.scale_normalized = None
        self.marked = True
        reclassgroups = reclasscode.match(campus_class_code)
        if reclassgroups:
            self.campus = reclassgroups.group("campus")
            self.year = int(reclassgroups.group("year"))
            self.subject = reclassgroups.group("subject")

        if self.mark and re.match(rescalescore, self.mark):
            self.scale = float(self.mark)
            self.scale_normalized = int(normalized)
        else:
            self.marked = False
            if mark == "Absent":
                self.absent = True
            elif mark == "Not Submitted":
                self.submitted = False
            elif mark == "Submitted":
                self.submitted = True
            else:
                self.marked = True

    @property
    def kla(self):
        return self.subject[0] if self.subject else None


with sshtunnel.SSHTunnelForwarder((config['ssh']['host'], config['ssh']['port']), ssh_username=config['ssh']['user'], ssh_password=config['ssh']['pass'], remote_bind_address=(config['sql']['host'], config['sql']['port'])) as tunnel:
    db = MySQLdb.connect(host=config['sql']['host'], user=config['sql']['user'], passwd=config['sql']['pass'], db=config['sql']['db'], port=tunnel.local_bind_port)
    cursor = db.cursor()
    cursor.execute("""SELECT user.synergy_id AS id, submission_box.id AS box_id, folder_code.code, submission_return.mark, submission_return.norm_mark AS mark_normalized, submission_box.name AS name FROM submission_return LEFT OUTER JOIN submission_box ON submission_return.box_id = submission_box.id LEFT OUTER JOIN folder_code ON submission_box.category = folder_code.folder_id LEFT OUTER JOIN user ON submission_return.owner = user.id WHERE folder_code.deleted_at IS NULL AND folder_code.code IS NOT NULL AND submission_box.submission_grade_group_id = 2 AND submission_box.mark_type = 2 ORDER BY submission_return.owner, submission_return.id DESC""")
    for row in cursor:
        result = Result(*row)
        if result.scale:
            if result.id not in student_box:
                student_box[result.id] = list()
            if result.id not in student_counts:
                student_counts[result.id] = {'absent': 0, 'not_submitted': 0, 'submitted': 0, 'marked': 0, 'unmarked': 0}
            if result.box not in student_box[result.id]:
                if result.submitted: student_counts[result.id]['submitted'] += 1
                else: student_counts[result.id]['not_submitted'] += 1
                if result.absent: student_counts[result.id]['absent'] += 1
                if result.marked:
                    student_counts[result.id]['marked'] += 1
                    student_box[result.id].append(result.box)
                    if result.campus == 'CE' and result.year == 9:
                        results_nine_scale.append(result)
                    else:
                        results_scale.append(result)
                else:
                    student_counts[result.id]['unmarked'] += 1
        cursor.close()

    cursor = db.cursor()
    cursor.execute("""SELECT user.synergy_id AS id, submission_box.id AS box_id, folder_code.code, submission_return.mark, submission_return.norm_mark AS mark_normalized, submission_box.name AS name FROM submission_return LEFT OUTER JOIN submission_box ON submission_return.box_id = submission_box.id LEFT OUTER JOIN folder_code ON submission_box.category = folder_code.folder_id LEFT OUTER JOIN user ON submission_return.owner = user.id WHERE folder_code.deleted_at IS NULL AND folder_code.code IS NOT NULL AND submission_return.norm_mark IS NOT NULL AND (submission_box.submission_grade_group_id != 2 OR submission_box.mark_type != 2) ORDER BY submission_return.owner, submission_return.id DESC""")
    for row in cursor:
        result = Result(*row)
        if not result.scale and result.mark:
            if result.id not in student_box:
                student_box[result.id] = list()
            if result.id not in student_counts:
                student_counts[result.id] = {'absent': 0, 'not_submitted': 0, 'submitted': 0, 'marked': 0, 'unmarked': 0}
            if result.box not in student_box[result.id]:
                if result.submitted: student_counts[result.id]['submitted'] += 1
                else: student_counts[result.id]['not_submitted'] += 1
                if result.absent: student_counts[result.id]['absent'] += 1
                if result.marked:
                    student_counts[result.id]['marked'] += 1
                    student_box[result.id].append(result.box)
                    if result.campus == 'CE' and result.year == 9:
                        results_nine.append(result)
                    else:
                        results.append(result)
                else:
                    student_counts[result.id]['unmarked'] += 1

students = {}

with open('students.csv', 'r') as csv:
    for line in csv.readlines():
        id, year, campus, house, name = line.rstrip().split(',', 4)
        students[id] = Student(id, campus, year, name, house)

with open("scale-score-results.csv", "w") as csv:
    csv.write("""id,box,campus,class,year,kla,subject,scale,scale_normalized,name\r\n""")
    for result in results_scale:
        csv.write("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}\r\n".format(result.id, result.box, result.campus, result.class_code, result.year, result.kla, result.subject, result.scale, result.scale_normalized, result.name))
        if result.id not in students:
            print(result.id)
        else:
            students[result.id].add_result(result)

with open("non-scale-score-results.csv", "w") as csv:
    csv.write("""id,box,campus,class,year,kla,subject,mark,normalized,name\r\n""")
    for result in results:
        csv.write("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}\r\n".format(result.id, result.box, result.campus, result.class_code, result.year, result.kla, result.subject, result.mark, result.normalized, result.name))
        if result.id not in students:
            print(result.id)
        else:
            students[result.id].add_result(result)

with open("scale-score-results_nine.csv", "w") as csv:
    csv.write("""id,box,campus,class,year,kla,subject,scale,scale_normalized,name\r\n""")
    for result in results_nine_scale:
        csv.write("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}\r\n".format(result.id, result.box, result.campus, result.class_code, result.year, result.kla, result.subject, result.scale, result.scale_normalized, result.name))
        if result.id not in students:
            print(result.id)
        else:
            students[result.id].add_result(result)

with open("non-scale-score-results_nine.csv", "w") as csv:
    csv.write("""id,box,campus,class,year,kla,subject,mark,normalized,name\r\n""")
    for result in results_nine:
        csv.write("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}\r\n".format(result.id, result.box, result.campus, result.class_code, result.year, result.kla, result.subject, result.mark, result.normalized, result.name))
        if result.id not in students:
            print(result.id)
        else:
            students[result.id].add_result(result)

with open("totals.csv", "w") as csv:
    csv.write("""id,submitted,not_submitted,absent,marked,unmarked\r\n""")
    for id, data in student_counts.items():
        csv.write("{0},{1},{2},{3},{4},{5}\r\n".format(id, data['submitted'], data['not_submitted'], data['absent'], data['marked'], data['unmarked']))
        if result.id not in students:
            print(result.id)
        else:
            students[result.id].add_result(result)

with open("averages.csv", "w") as csv:
    csv.write("ID,Name,House,Campus,Year Level,Average Scale,Count Scale,Average Non-Scale,Count Non-Scale,English Year,English Scale Average,English Scale Count,English Non-Scale Average,English Non-Scale Count,Mathematics Year,Mathematics Scale Average,Mathematics Scale Count,Mathematics Non-Scale Average,Mathematics Non-Scale Count,Science Year,Science Scale Average,Science Scale Count,Science Non-Scale Average,Science Non-Scale Count,Language Year,Language Scale Average,Language Scale Count,Language Non-Scale Average,Language Non-Scale Count,Technology Year,Technology Scale Average,Technology Scale Count,Technology Non-Scale Average,Technology Non-Scale Count,PE Year,PE Scale Average, PE Scale Count,PE Non-Scale Average, PE Non-Scale Count,Arts Year,Arts Scale Average,Arts Scale Count,Arts Non-Scale Average,Arts Non-Scale Count,Humanities Year,Humanities Scale Average, Humanities Scale Count,Humanities Non-Scale Average, Humanities Non-Scale Count,RE Year,RE Scale Average,RE Scale Count,RE Non-Scale Average,RE Non-Scale Count\r\n")
with open("averages_nine.csv", "w") as csv:
    csv.write("ID,Name,House,COM Average,COM Count,CHAL Average,CHAL Count,HUME Average,HUME Count,STEM Average,STEM Count,Language Average,Language Count,Technology Average,Technology Count,Arts Average,Arts Count\r\n")

ninece = []
for id, student in students.items():
    counts = student.scale_counts_bykla
    averages = student.scale_avg_bykla
    if student.campus == 'CE' and student.year is 9:
        ninece += [x.subject for x in student.results]

        print(averages)
        with open("averages_nine.csv", "a") as csv:
            csv.write("{},\"{}\",{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\r\n".format(student.id, student.name, student.house, averages['COM'] if 'COM' in averages else None, counts['COM'] if 'COM' in counts else 0, averages['CHAL'] if 'CHAL' in averages else None, counts['CHAL'] if 'CHAL' in counts else 0, averages['HUME'] if 'HUME' in averages else None, counts['HUME'] if 'HUME' in counts else 0, averages['STEM'] if 'STEM' in averages else None, counts['STEM'] if 'STEM' in counts else 0, averages['L'] if 'L' in averages else None, counts['L'] if 'L' in counts else 0, averages['T'] if 'T' in averages else None, counts['T'] if 'T' in counts else 0, averages['A'] if 'A' in averages else None, counts['A'] if 'A' in counts else 0))
    else:
        ncounts = student.norm_counts_bykla
        naverages = student.norm_avg_bykla

        avg = {}
        avg['s'] = {}
        avg['n'] = {}
        count = {}
        count['s'] = {}
        count['n'] = {}

 
        avg['s']['e'] = averages['E'] if 'E' in averages else 0
        count['s']['e'] = counts['E'] if 'E' in counts else 0
        avg['s']['m'] = averages['M'] if 'M' in averages else 0
        count['s']['m'] = counts['M'] if 'M' in counts else 0
        avg['s']['s'] = averages['S'] if 'S' in averages else 0
        count['s']['s'] = counts['S'] if 'S' in counts else 0
        avg['s']['l'] = averages['L'] if 'L' in averages else 0
        count['s']['l'] = counts['L'] if 'L' in counts else 0
        avg['s']['t'] = averages['T'] if 'T' in averages else 0
        count['s']['t'] = counts['T'] if 'T' in counts else 0
        avg['s']['p'] = averages['P'] if 'P' in averages else 0
        count['s']['p'] = counts['P'] if 'P' in counts else 0
        avg['s']['a'] = averages['A'] if 'A' in averages else 0
        count['s']['a'] = counts['A'] if 'A' in counts else 0
        avg['s']['h'] = averages['H'] if 'H' in averages else 0
        count['s']['h'] = counts['H'] if 'H' in counts else 0
        avg['s']['r'] = averages['R'] if 'R' in averages else 0
        count['s']['r'] = counts['R'] if 'R' in counts else 0
        avg['n']['e'] = naverages['E'] if 'E' in naverages else 0
        count['n']['e'] = ncounts['E'] if 'E' in ncounts else 0
        avg['n']['m'] = naverages['M'] if 'M' in naverages else 0
        count['n']['m'] = ncounts['M'] if 'M' in ncounts else 0
        avg['n']['s'] = naverages['S'] if 'S' in naverages else 0
        count['n']['s'] = ncounts['S'] if 'S' in ncounts else 0
        avg['n']['l'] = naverages['L'] if 'L' in naverages else 0
        count['n']['l'] = ncounts['L'] if 'L' in ncounts else 0
        avg['n']['t'] = naverages['T'] if 'T' in naverages else 0
        count['n']['t'] = ncounts['T'] if 'T' in ncounts else 0
        avg['n']['p'] = naverages['P'] if 'P' in naverages else 0
        count['n']['p'] = ncounts['P'] if 'P' in ncounts else 0
        avg['n']['a'] = naverages['A'] if 'A' in naverages else 0
        count['n']['a'] = ncounts['A'] if 'A' in ncounts else 0
        avg['n']['h'] = naverages['H'] if 'H' in naverages else 0
        count['n']['h'] = ncounts['H'] if 'H' in ncounts else 0
        avg['n']['r'] = naverages['R'] if 'R' in naverages else 0
        count['n']['r'] = ncounts['R'] if 'R' in ncounts else 0

        scales = []
        norms = []

        count_scales = 0
        count_norms = 0

        for kla, scale in avg['s'].items():
            if scale is not 0:
                scales.append(scale)
                count_scales += count['s'][kla]
        for kla, norm in avg['n'].items():
            if norm is not 0:
                norms.append(scale)
                count_norms += count['n'][kla]

        kla_years = student.year_level_bykla
        avg_scales = sum(scales) / len(scales) if len(scales) > 0 else 0
        avg_norms = sum(norms) / len(norms) if len(norms) > 0 else 0

        with open("averages.csv", "a") as csv:
            csv.write("{},\"{}\",{},{},{},{},{},{},{},{kla_years[E]},{avg[s][e]},{count[s][e]},{avg[n][e]},{count[n][e]},{kla_years[M]},{avg[s][m]},{count[s][m]},{avg[n][m]},{count[n][m]},{kla_years[S]},{avg[s][s]},{count[s][s]},{avg[n][s]},{count[n][s]},{kla_years[L]},{avg[s][l]},{count[s][l]},{avg[n][l]},{count[n][l]},{kla_years[T]},{avg[s][t]},{count[s][t]},{avg[n][t]},{count[n][t]},{kla_years[P]},{avg[s][p]},{count[s][p]},{avg[n][p]},{count[n][p]},{kla_years[A]},{avg[s][a]},{count[s][a]},{avg[n][a]},{count[n][a]},{kla_years[H]},{avg[s][h]},{count[s][h]},{avg[n][h]},{count[n][h]},{kla_years[R]},{avg[s][r]},{count[s][r]},{avg[n][r]},{count[n][r]}\r\n".format(student.id, student.name, student.house, student.campus, student.year, avg_scales, count_scales, avg_norms, count_norms, avg=avg, count=count, kla_years=kla_years))
