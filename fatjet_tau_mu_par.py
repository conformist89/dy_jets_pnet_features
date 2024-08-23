import concurrent.futures
import uproot
import matplotlib.pyplot as plt
from XRootD import client
import argparse

parser = argparse.ArgumentParser("simple_example")
parser.add_argument("counter", help="An integer will be increased by 1 and printed.", type=int)
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
remote_path = '/store/user/olavoryk/CROWN/ntuples/boost_DY_18UL_6Aug_fj_m_tau_dr_v2/CROWNRun/2018/'

def process_sample(sa):
    remote_files = list_remote_files(remote_redirector, remote_path + sa + channel + "/")
    return sa, remote_files

with concurrent.futures.ThreadPoolExecutor() as executor:
    future_to_sample = {executor.submit(process_sample, sa): sa for sa in samples}
    for future in concurrent.futures.as_completed(future_to_sample):
        sa, remote_files = future.result()
        ntuples[sa] = remote_files



def load_events(file_names):
    return uproot.lazy(file_names, filter_name="gen_mu_tau_deltaR_with_fj")


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


plt.hist(events['fatjet_mu_tau_deltaR'], bins = 100)
# plt.hist(events['gen_mu_tau_deltaR_with_fj'], bins = 100)

plt.xlabel(r"$\Delta$ R (fatjet && gen $\tau_{had} \tau_{\mu} pair)$")
# plt.xlabel(r"$\Delta$ R ( gen $\tau_{had}$ && gen $\tau_{\mu}$ pair)")

plt.ylabel("dN")
plt.xlim(0, 10)
plt.ylim(0, 0.6e6)
# plt.savefig("fatjet_tau_mu_dr_par_cleanted_fatjets_zp_incl.png")
plt.savefig("fatjet_tau_mu_dr_par_cleanted_fatjets_zp_pnet_cut.png")
# plt.savefig("gen_tau_mu_dr_par_cleanted_fatjets_zp_incl.png")