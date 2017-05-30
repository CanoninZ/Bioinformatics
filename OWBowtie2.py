import sys, os, fnmatch
import re
from Orange.widgets import widget, gui
from orangebiodepot.util.DockerClient import DockerClient, PullImageThread
from PyQt5.QtCore import QThread, pyqtSignal

#bowtie2 [options]* -x <bt2-idx> {-1 <m1> -2 <m2> | -U <r>} -S [<hit>]

class OWSBowtie2(widget.OWWidget):
    RunMode_GenerateGenome = 0
    RunMode_STAR = 1

    name = "Bowtie2"
    description = "An ultrafast memory-efficient short read aligner"
    category = "RNASeq"
    icon = "icons/bowtie.png"

    priority = 10

    inputs = [("FQ Files", str, "setFastqInput", widget.Default), 
              ("Bt2 Index", str, "setStarIndexDir")]
    outputs = [("SAM Files", str)]

    want_main_area = False

    dockerClient = DockerClient('unix:///var/run/docker.sock', 'local')
    
    image_name = "biocontainers/bowtie2"
    image_version = "latest"


    def __init__(self):

        super().__init__()

        self.bFastqDirSet = False
        self.bStarIndexDirSet = False

        # GUI
        box = gui.widgetBox(self.controlArea, "Info")
        self.info_fastq = gui.widgetLabel(box, 'Please specify a directory with FQ files.')
        self.info_fastq.setWordWrap(True)
       
        self.AutoBowtie2 = False
        
        self.info_starindex = gui.widgetLabel(box, 'Please specify a directory with bt2 index.')
        
        self.info_starindex.setWordWrap(True)

        gui.checkBox(self.controlArea, self, 'AutoBowtie2', 'Run automatically when directory was set.')

        self.btnStartBowtie2 = gui.button(self.controlArea, self, "Star Bowtie2", callback=self.StartBowtie2)
        self.btnStartBowtie2.setEnabled(False)

    def setFastqInput(self, path):
        # When a user removes a connected Directory widget,
        # it sends a signal with path=None
        if path is None:
            self.bFastqDirSet = False
        else:
            if not os.path.exists(path):
                self.bFastqDirSet = False
                print('References set to invalid directory')
            else:
                self.fastqDirectory = path
                self.bFastqDirSet = True
                self.info_fastq.setText("FQ files: {0}".format(self.fastqDirectory))

        self.btnStartBowtie2.setEnabled(self.bFastqDirSet)

        if self.bFastqDirSet and self.AutoBowtie2  and self.bStarIndexDirSet:
            self.StartBowtie2()



    def setStarIndexDir(self, path):
        if path is None:
            self.bStarIndexDirSet = False
        else:
            if not os.path.exists(path):
                self.bStarIndexDirSet = False
                print('References set to invalid directory')
            else:
                self.starindexDirectory = path
                self.bStarIndexDirSet = True
                self.info_starindex.setText("Bt2 index: {0}".format(self.starindexDirectory))
        self.btnStartBowtie2.setEnabled(self.bFastqDirSet and self.bStarIndexDirSet)

        if self.bFastqDirSet and self.bStarIndexDirSet and self.AutoBowtie2:
            self.StartBowtie2()



    def StartBowtie2(self):
        if not self.bFastqDirSet or not self.bStarIndexDirSet:
            return

        if not self.dockerClient.has_image(self.image_name, self.image_version):
            self.pull_image()
        elif self.bFastqDirSet:
            self.run_startbowtie2()

    """
        Pull image
    """
    def pull_image(self):
        self.info_fastq.setText('Pulling \'' + self.image_name + ":" + self.image_version)
        self.info_starindex.setText('')
        self.setStatusMessage("Downloading...")
        self.progressBarInit()
        self.btnStartBowtie2.setEnabled(False)
        # Pull the image in a new thread
        self.pull_image_thread = PullImageThread(self.dockerClient, self.image_name, self.image_version)
        self.pull_image_thread.pull_progress.connect(self.pull_image_progress)
        self.pull_image_thread.finished.connect(self.pull_image_done)
        self.pull_image_thread.start()

    def pull_image_progress(self, val=0):
        self.progressBarSet(val)

    def pull_image_done(self):
        self.info_fastq.setText('Finished pulling \'' + self.image_name + ":" + self.image_version + '\'')
        self.progressBarFinished()
        self.run_startbowtie2()

    def run_startbowtie2(self):
        self.btnStartBowtie2.setEnabled(False)
        self.info_fastq.setText('Running bowtie alignment...')
        self.setStatusMessage('Running...')
        #self.progressBarInit()
        # Run the container in a new thread
        self.run_startbowtie2_thread = Bowtie2Thread(self.dockerClient,
                                                     self.image_name,
                                                     self.image_version,
                                                     self.fastqDirectory,
                                                     self.starindexDirectory)

        self.run_startbowtie2_thread.analysis_progress.connect(self.run_startbowtie2_progress)
        self.run_startbowtie2_thread.finished.connect(self.run_startbowtie2_done)
        self.run_startbowtie2_thread.start()


    def run_startbowtie2_progress(self, val):
        self.progressBarSet(val)


    def run_startbowtie2_done(self):
        self.info_fastq.setText("Finished run bowtie2")
        self.btnStartBowtie2.setEnabled(True)
        self.btnStartBowtie2.setText('Run again')
        self.setStatusMessage('Finished!')
        output_channel = self.fastqDirectory
        self.send("SAM Files", output_channel)



class Bowtie2Thread(QThread):
    analysis_progress = pyqtSignal(int)

    container_fastq_dir = '/data/fastq'


    container_index_dir = '/data/index'

    # biocontainers/bowtie2
    def __init__(self, cli, image_name, image_version, host_fastq_dir, host_index_dir):
        QThread.__init__(self)
        self.docker = cli
        self.image_name = image_name
        self.image_version = image_version
        self.host_fastq_dir = host_fastq_dir
        self.host_index_dir = host_index_dir


    def __del__(self):
        self.wait()

    """
    Run should first create a container and then start it
    """
    def run(self):
        # search fastq files
        fastq_files = [os.path.join(self.container_fastq_dir, x) for x in os.listdir(self.host_fastq_dir)]
        index_files = [os.path.basename(x) for x in fnmatch.filter(os.listdir(self.host_index_dir), "*.bt2")]
        index_fileslist = []
        for f in index_files:
            #basename, _ = os.path.splitext(f)

            index_fileslist.append(f.split('.')[0])
        print(index_fileslist)

        index_fileslist = ','.join(x for x in list(set(index_fileslist)))
        print(index_fileslist)

        r1, r2 = [], []
        # Pattern convention: Look for "R1" / "R2" in the filename, or "_1" / "_2" before the extension
        pattern = re.compile('(?:^|[._-])(R[12]|[12]\.f)')
        for fastq in sorted(fastq_files):
            match = pattern.search(os.path.basename(fastq))
            if not match:
                print('Invalid FASTQ file: {0}, ignore it.'.format(fastq))
                continue
            elif '1' in match.group():
                r1.append(fastq)
            elif '2' in match.group():
                r2.append(fastq)

        if len(r1) != len(r2):
            print('Check fastq names, uneven number of pairs found.\nr1: {}\nr2: {}'.format(r1, r2))
            return

        if len(r1) == 0:
            if len(fastq_files) == 0:
                return
            else:
                r1 = fnmatch.filter(fastq_files, '*.fq')

        r1 = [os.path.join(self.container_fastq_dir, x) for x in r1]
        r2 = [os.path.join(self.container_fastq_dir, x) for x in r2]
        r1_filelist = ','.join(c for c in r1)
        r2_filelist = ','.join(c for c in r2)


        #bowtie2 [options]* -x <bt2-idx> {-1 <m1> -2 <m2> | -U <r>} -S [<hit>]
        parameters = ['bowtie2']

        parameters.extend(['-x', os.path.join(self.container_index_dir, index_fileslist) ])
        if len(r2) == 0:    # unpaired mode
            parameters.extend(['-U', r1_filelist])
        else:
            parameters.extend(['-1', r1_filelist])
            parameters.extend(['-2', r2_filelist])

        
        parameters.extend(['-S', os.path.join(self.container_index_dir, 'result_test.sam')])

        commands = "bash -c \"" + ' '.join((str(w) for w in parameters)) + "\""
        print(commands)


        volumes = {self.host_fastq_dir: self.container_fastq_dir,self.host_index_dir: self.container_index_dir}


        response = self.docker.create_container(self.image_name+":"+self.image_version,
                                                volumes=volumes,
                                                commands=commands)
        if response['Warnings'] == None:
            self.containerId = response['Id']
            self.docker.start_container(self.containerId)
        else:
            print(response['Warnings'])

        # Keep running until container is exited
        while self.docker.container_running(self.containerId):
            self.sleep(1)
        # Remove the container now that it is finished
        self.docker.remove_container(self.containerId)

def main(argv=sys.argv):
    from AnyQt.QtWidgets import QApplication
    app = QApplication(list(argv))

    ow = OWSBowtie2()
    ow.setFastqInput("/Users/zoeyz/Documents/bioseminar/data/")
    ow.setStarIndexDir("/Users/zoeyz/Documents/bioseminar/data/index/")     

    ow.show()
    ow.raise_()

    ow.handleNewSignals()
    app.exec_()
    ow.setFastqInput(None)
    ow.handleNewSignals()
    return 0

if __name__=="__main__":
    sys.exit(main())
