import concurrent.futures
import uproot
import matplotlib.pyplot as plt
from XRootD import client
import argparse

parser = argparse.ArgumentParser("simple_example")
parser.add_argument("--quantity", help="Distribution you want to plot", type=str)
parser.add_argument("--tag", help="Ntuple production tag", type=str)
args = parser.parse_args()

def list_remote_files(remote_redirector, remote_path):
    # Create a FileSystem object
    fs = client.FileSystem(remote_redirector)

    # List the contents of the remote directory
    status, listing = fs.dirlist(remote_path)

    root_files = []
    if status.ok:
        for entry in listing:
            if entry.name.endswith(".root"):
                root_files.append(remote_path + "/" + entry.name)
    else:
        print(f"Failed to list directory {remote_path}: {status.message}")
    return root_files

samples = [
    "DYJetsToLL_LHEFilterPtZ-100To250_MatchEWPDG20_TuneCP5_13TeV-amcatnloFXFX-pythia8RunIISummer20UL18NanoAODv12-106X/",
    "DYJetsToLL_LHEFilterPtZ-250To400_MatchEWPDG20_TuneCP5_13TeV-amcatnloFXFX-pythia8RunIISummer20UL18NanoAODv12-106X/",
    "DYJetsToLL_LHEFilterPtZ-400To650_MatchEWPDG20_TuneCP5_13TeV-amcatnloFXFX-pythia8RunIISummer20UL18NanoAODv12-106X/",
    "DYJetsToLL_LHEFilterPtZ-650ToInf_MatchEWPDG20_TuneCP5_13TeV-amcatnloFXFX-pythia8RunIISummer20UL18NanoAODv12-106X/"
]

ntuples = {}
channel = 'mt'
remote_redirector = 'root://cmsdcache-kit-disk.gridka.de:1094/'
 
remote_path = "/store/user/olavoryk/CROWN/ntuples/{folder}/CROWNRun/2018/".format(folder=str(args.tag))

def process_sample(sa):
    remote_files = list_remote_files(remote_redirector, remote_path + sa + channel + "/")
    return sa, remote_files

with concurrent.futures.ThreadPoolExecutor() as executor:
    future_to_sample = {executor.submit(process_sample, sa): sa for sa in samples}
    for future in concurrent.futures.as_completed(future_to_sample):
        sa, remote_files = future.result()
        ntuples[sa] = remote_files



def load_events(file_names):
    return uproot.lazy(file_names, filter_name=args.quantity)


file_names = []
for key, value in ntuples.items():
    for v in value:
        file_names.append(remote_redirector + v + ":ntuple")

with concurrent.futures.ThreadPoolExecutor() as executor:
    future_to_files = {executor.submit(load_events, file_names): file_names}
    for future in concurrent.futures.as_completed(future_to_files):
        events = future.result()


SMALL_SIZE = 12
MEDIUM_SIZE = 16
BIGGER_SIZE = 18

plt.rc('font', size=SMALL_SIZE)          # controls default text sizes
plt.rc('axes', titlesize=SMALL_SIZE)     # fontsize of the axes title
plt.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('legend', fontsize=SMALL_SIZE)    # legend fontsize
plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title


plt.hist(events[args.quantity], bins = 100)

label = ''
xlim_up = 0
xlim_down = 0

if args.quantity == "fatjet_mu_tau_deltaR":
    label = r"$\Delta$ R (fatjet && gen $\tau_{had} \tau_{\mu} pair)$"
    xlim_up = 10
    xlim_down = 0
if args.quantity == "gen_mu_tau_deltaR_with_fj":
    label = r"$\Delta$ R ( gen $\tau_{had}$ && gen $\tau_{\mu}$ pair)"
    xlim_up = 10
    xlim_down = 0
if args.quantity == "fatjet_mu_tau_deltaPhi":
    label = r'$\Delta$ R (fatjet && gen $\tau_{had} \tau_{\mu} pair)$'
    xlim_up = 5
    xlim_down = -5

plt.xlabel(label)

plt.ylabel("dN")
plt.xlim(xlim_down, xlim_up)
plt.ylim(0, 0.6e6)

plt.savefig(args.quantity+"_zp_incl.pdf")