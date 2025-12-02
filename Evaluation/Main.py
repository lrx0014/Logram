import os
import time
from DictionarySetUp import dictionaryBuilder
from MatchToken import tokenMatch
from evaluator import evaluate

HDFS_format = '<Date> <Time> <Pid> <Level> <Component>: <Content>'  # HDFS log format
Andriod_format = '<Date> <Time>  <Pid>  <Tid> <Level> <Component>: <Content>' #Andriod log format
Spark_format = '<Date> <Time> <Level> <Component>: <Content>'#Spark log format
Zookeeper_format = '<Date> <Time> - <Level>  \[<Node>:<Component>@<Id>\] - <Content>' #Zookeeper log format
Windows_format = '<Date> <Time>, <Level>                  <Component>    <Content>' #Windows log format
Thunderbird_format = '<Label> <Timestamp> <Date> <User> <Month> <Day> <Time> <Location> <Component>(\[<PID>\])?: <Content>' #Thunderbird_format
Apache_format = '\[<Time>\] \[<Level>\] <Content>' #Apache format
BGL_format = '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>' #BGL format
Hadoop_format = '<Date> <Time> <Level> \[<Process>\] <Component>: <Content>' #Hadoop format
HPC_format = '<LogId> <Node> <Component> <State> <Time> <Flag> <Content>' #HPC format
Linux_format = '<Month> <Date> <Time> <Level> <Component>(\[<PID>\])?: <Content>' #Linux format
Mac_format = '<Month>  <Date> <Time> <User> <Component>\[<PID>\]( \(<Address>\))?: <Content>' #Mac format
OpenSSH_format = '<Date> <Day> <Time> <Component> sshd\[<Pid>\]: <Content>' #OpenSSH format
OpenStack_format = '<Logrecord> <Date> <Time> <Pid> <Level> <Component> \[<ADDR>\] <Content>' #OpenStack format
HealthApp_format = '<Time>\|<Component>\|<Pid>\|<Content>'
Proxifier_format = '\[<Time>\] <Program> - <Content>'


def test_parsing(name, log_format, regex, double_threshold, trible_threshold, path_to_log, path_to_structured_log, path_to_template):
    """
    Run parsing for a dataset and report runtime, group F1, and template (per-line) accuracy.
    """
    out_dir = os.path.dirname(path_to_template)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    start = time.perf_counter()
    double_dict, tri_dict, all_tokens = dictionaryBuilder(log_format, path_to_log, regex)
    tokenMatch(all_tokens, double_dict, tri_dict, double_threshold, trible_threshold, path_to_template)
    f_measure, accuracy = evaluate(path_to_structured_log, f"{path_to_template}Event.csv")
    elapsed = time.perf_counter() - start

    print(f"[{name}] Elapsed: {elapsed:.3f}s, Group F1: {f_measure:.4f}, Template Accuracy: {accuracy:.4f}")
    return elapsed, f_measure, accuracy

HDFS_Regex = [
        r'blk_(|-)[0-9]+' , # block id
        r'(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)', # IP
        r'(?<=[^A-Za-z0-9])(\-?\+?\d+)(?=[^A-Za-z0-9])|[0-9]+$', # Numbers
]
Hadoop_Regex = [r'(\d+\.){3}\d+']
Spark_Regex = [r'(\d+\.){3}\d+', r'\b[KGTM]?B\b', r'([\w-]+\.){2,}[\w-]+']
Zookeeper_Regex = [r'(/|)(\d+\.){3}\d+(:\d+)?']
BGL_Regex = [r'core\.\d+']
HPC_Regex = [r'=\d+']
Thunderbird_Regex = [r'(\d+\.){3}\d+']
Windows_Regex = [r'0x.*?\s']
Linux_Regex = [r'(\d+\.){3}\d+', r'\d{2}:\d{2}:\d{2}']
Andriod_Regex = [r'(/[\w-]+)+', r'([\w-]+\.){2,}[\w-]+', r'\b(\-?\+?\d+)\b|\b0[Xx][a-fA-F\d]+\b|\b[a-fA-F\d]{4,}\b']
Apache_Regex = [r'(\d+\.){3}\d+']
OpenSSH_Regex = [r'(\d+\.){3}\d+', r'([\w-]+\.){2,}[\w-]+']
OpenStack_Regex = [r'((\d+\.){3}\d+,?)+', r'/.+?\s', r'\d+']
Mac_Regex = [r'([\w-]+\.){2,}[\w-]+']
HealthApp_Regex = []
Proxifier_Regex = [r'<\d+\ssec', r'([\w-]+\.)+[\w-]+(:\d+)?', r'\d{2}:\d{2}(:\d{2})*', r'[KGTM]B']

# max = 0
# maxd = 1
# maxt = 1
#
# for t in range(1,101):
#     for d in range(t,101):
#         doubleDictionaryList, triDictionaryList, allTokenList = dictionaryBuilder(Andriod_format, 'TestLogs/Andriod_2k.log', Andriod_Regex)
#         tokenMatch(allTokenList,doubleDictionaryList,triDictionaryList,d,t,'Output/')
#         f_measure, accuracy = evaluate('GroundTruth/Andriod_2k.log_structured.csv', 'Output/event.csv')
#         if accuracy > max or max == 0:
#             max = accuracy
#             maxd = d
#             maxt = t
#         doubleDictionaryList.clear()
#         triDictionaryList.clear()
#         allTokenList.clear()
#
if __name__ == "__main__":
    # We use an automatic approach to gain the threshold. The parameters listed below are suggested thresholds for different datasets.
    # Andriod: 14, 13
    # Apache: 75, 32
    # BGL: 18, 10
    # HDFS: 15, 15
    # Hadoop: 9, 6
    # HPC: 13, 11
    # Linux: 32, 24
    # Mac: 11, 10
    # OpenSSH: 47, 80
    # OpenStack: 16, 9
    # Proxifier: 115, 95
    # Spark: 37, 37
    # Thunderbird: 8, 7
    # Windows: 16, 16
    # Zookeeper: 9, 9
    # HealthApp: 23, 5

    test_parsing(
        name="Apache",
        log_format=Apache_format,
        regex=Apache_Regex,
        double_threshold=75,
        trible_threshold=32,
        path_to_log='test_logs/Apache/Apache_full.log',
        path_to_structured_log='test_logs/Apache/Apache_full.log_structured.csv',
        path_to_template='test_logs/Apache/Apache_full.log_templates.csv'
    )

    test_parsing(
        name="Linux",
        log_format=Linux_format,
        regex=Linux_Regex,
        double_threshold=32,
        trible_threshold=24,
        path_to_log='test_logs/Linux/Linux_full.log',
        path_to_structured_log='test_logs/Linux/Linux_full.log_structured.csv',
        path_to_template='test_logs/Linux/Linux_full.log_templates.csv'
    )

    test_parsing(
        name="OpenSSH",
        log_format=OpenSSH_format,
        regex=OpenSSH_Regex,
        double_threshold=47,
        trible_threshold=80,
        path_to_log='test_logs/OpenSSH/OpenSSH_full.log',
        path_to_structured_log='test_logs/OpenSSH/OpenSSH_full.log_structured.csv',
        path_to_template='test_logs/OpenSSH/OpenSSH_full.log_templates.csv'
    )

    test_parsing(
        name="Zookeeper",
        log_format=Zookeeper_format,
        regex=Zookeeper_Regex,
        double_threshold=9,
        trible_threshold=9,
        path_to_log='test_logs/Zookeeper/Zookeeper_full.log',
        path_to_structured_log='test_logs/Zookeeper/Zookeeper_full.log_structured.csv',
        path_to_template='test_logs/Zookeeper/Zookeeper_full.log_templates.csv'
    )

    test_parsing(
        name="HDFS",
        log_format=HDFS_format,
        regex=HDFS_Regex,
        double_threshold=15,
        trible_threshold=15,
        path_to_log='test_logs/HDFS/HDFS_full.log',
        path_to_structured_log='test_logs/HDFS/HDFS_full.log_structured.csv',
        path_to_template='test_logs/HDFS/HDFS_full.log_templates.csv'
    )
