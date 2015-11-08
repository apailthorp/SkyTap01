import argparse
import datetime
import json
import sys
import time
import requests

protocol = 'https://'
envEnd = protocol + 'cloud.skytap.com/configurations'
vmEnd = protocol + 'cloud.skytap.com/vms'

# How long to wait in seconds between polls of a VM to see if it has changed state
pollInterval = 1
# How many times to re-poll before giving up
pollLimit = 60


def authGetJson(args, url):
    headers = {'Accept': 'application/json'}
    try:
        r = requests.get(url, auth=requests.auth.HTTPBasicAuth(args.username, args.token), headers=headers)
        if r.status_code in [401, 404, 409, 422, 423, 429, 500]:
            return 'Status {0}: {1}'.format(r.status_code, r.json())
        if (r.status_code // 100 == 2):
            return r.json()
    except requests.exceptions.RequestException as e:
        return 'Error: {}'.format(e)


def authPutJson(args, url, jsonbody):
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    try:
        r = requests.put(url, auth=requests.auth.HTTPBasicAuth(args.username, args.token), headers=headers,
                         data=jsonbody)
        if r.status_code in [401, 404, 409, 422, 423, 429, 500]:
            return 'Status {0}: {1}'.format(r.status_code, r.json())
        if (r.status_code // 100 == 2):
            return r
    except requests.exceptions.RequestException as e:
        return 'Error: {}'.format(e)


def getEnvs(args):
    env = authGetJson(args, envEnd)
    if type(env) == str:
        reportMessage(env)
        sys.exit(1)
    return env


def getEnv(args, envID):
    someEnvEnd = envEnd + '/' + envID + '.json'
    env = authGetJson(args, someEnvEnd)
    if type(env) == str:
        reportMessage(env)
        sys.exit(1)
    return env


def getEnvVMs(args, env):
    someConfig = getEnv(args, env)
    someVMs = someConfig['vms']
    return someVMs


def getVM(args, vm):
    someVMEnd = vmEnd + '/' + vm + '.json'
    someVM = authGetJson(args, someVMEnd)
    if type(someVM) == str:
        reportMessage(someVM)
        sys.exit(1)
    return someVM


def setVMRunstate(args, vmID, runstate):
    someVM = getVM(args, vmID)
    reportVMRunstateChange(someVM, runstate)
    currentRunstate = someVM['runstate']
    if currentRunstate != runstate:
        someVMEnd = vmEnd + '/' + vmID + '.json'
        runStateJson = json.dumps({'runstate': runstate})
        someVM = authPutJson(args, someVMEnd, runStateJson)
        if type(someVM) == str:
            reportMessage(someVM)
            sys.exit(1)


def sleepPollVMRunstate(args, vmID, runstate):
    # If there is a way to avoid status 423, this might be a candidate to run with multiprocessing
    someVM = getVM(args, vmID)
    currentRunstate = someVM['runstate']
    pollCount = 0
    busySeen = False
    # break out of loop if poll limit reached, current runstate become runstate,
    # or runstate becomes busy then  transitions to new state
    while pollCount < pollLimit and currentRunstate != runstate and not (busySeen and currentRunstate != 'busy'):
        time.sleep(pollInterval)
        someVM = getVM(args, vmID)
        currentRunstate = someVM['runstate']
        pollCount += 1
        reportVM(someVM)
        if currentRunstate == 'busy':
            busySeen = True
    if currentRunstate != runstate:
        reportMessage('ERROR setting {0} to runstate={1}'.format(vmID, runstate))
    else:
        reportMessage('VM {0} is in runstate {1}. Have a nice day.'.format(vmID, currentRunstate))


def setVMListRunstate(args, vms, newRunstate, endRunstate=None):
    if endRunstate == None:
        endRunstate = newRunstate
    for someVM in vms:
        someVMID = someVM['id']
        # it would be nice to run more than one of these at a time, but response 423 happens
        setVMRunstate(args, someVMID, newRunstate)
        sleepPollVMRunstate(args, someVMID, endRunstate)


def setVMMultiListRunstate(args, envID, vms, newRunstate, endRunstate=None):
    if endRunstate == None:
        endRunstate = newRunstate
    vmIDList = []
    for someVM in vms:
        someVMID = someVM['id']
        vmIDList.append(someVMID)
    multiBody = json.dumps({'multiselect': vmIDList, 'runstate': newRunstate})
    someEnvEnd = envEnd + '/' + envID + '.json'
    someVM = authPutJson(args, someEnvEnd, multiBody)
    if type(someVM) == str:
        reportMessage(someVM)
        sys.exit(1)
    retryvms = sleepPollVMListRunstate(args, envID, vms, endRunstate)
    if retryvms != None:
        reportMessage('ERROR: Not all VMs set as requested in multirequest, trying single requests')
        setVMListRunstate(args, vms, newRunstate, endRunstate)


def sleepPollVMListRunstate(args, envID, vms, runstate):
    someEnv = getEnv(args, envID)
    vmListCount = len(vms)
    pollCount = 0
    busyseenvms = []
    failedvms = []
    completeCount = 0
    # break out of loop if poll limit reached, current runstate become runstate,
    # or runstate becomes busy then  transitions to new state
    while pollCount < pollLimit and completeCount < vmListCount:
        time.sleep(pollInterval)
        someEnv = getEnv(args, envID)
        pollCount += 1
        allVMs = someEnv['vms']
        reportVMs(allVMs)
        vmsIDs = []
        for someVM in vms:
            vmsIDs.append(someVM['id'])
        for someVM in allVMs:
            someVMId = someVM['id']
            if someVMId in vmsIDs:
                someVmRunstate = someVM['runstate']
                if someVmRunstate == runstate or (someVMId in busyseenvms and someVmRunstate != 'busy'):
                    completeCount += 1
                    if someVmRunstate != runstate:
                        failedvms.append(someVMId)
                    if someVMId in busyseenvms:
                        busyseenvms.remove(someVMId)
                if someVmRunstate == 'busy' and (someVMId not in busyseenvms):
                    busyseenvms.append(someVMId)
    if len(failedvms) > 0:
        return failedvms


def reportMessage(message):
    print message


def reportEnv(env):
    envID = env['id']
    someEnvName = env['name']
    print envID + " - " + someEnvName + ":"


def reportVM(vm):
    vmId = vm['id']
    vmName = vm['name']
    vmRunstate = vm['runstate']
    timeStamp = datetime.datetime.now().time().isoformat()
    vmListLine = "\t{0:18}{1:12}{2:50s}{3:>15s}".format(timeStamp, vmId, vmName, vmRunstate)
    print vmListLine


def reportVMs(vms):
    for someVM in vms:
        reportVM(someVM)
    print "--------------------------------------------"


def reportVMRunstateChange(vm, runstate):
    print "Attempting to set VM {0} to runstate: {1}".format(vm['id'], runstate)
    reportVM(vm)


commandtable = {'list': None,
                'start': {'runmulti': True, 'acceptrunstates': ['stopped'], 'newrunstate': 'running', 'finalrunstate': 'running'},
                'suspend': {'runmulti': True, 'acceptrunstates': ['running'], 'newrunstate': 'suspended', 'finalrunstate': 'suspended'},
                'resume': {'runmulti': True, 'acceptrunstates': ['suspended'], 'newrunstate': 'running', 'finalrunstate': 'running'},
                'stop': {'runmulti': True, 'acceptrunstates': ['running'], 'newrunstate': 'stopped', 'finalrunstate': 'stopped'},
                'halt': {'runmulti': True, 'acceptrunstates': ['suspended', 'running'], 'newrunstate': 'halted', 'finalrunstate': 'stopped'},
                'restart': {'runmulti': False, 'acceptrunstates': ['running'], 'newrunstate': 'restarted', 'finalrunstate': 'running'}
                }


def setupArgParser():
    # Set up to handle arguments, provide help
    runHelp = """
    Sample code to execute REST calls against SkyTap APIs.
    Example call:
        python envDo.py -u myUserName -t myAPISecurityToken
    Check the My Account tab to find a current API Security Token value.
    """
    parser = argparse.ArgumentParser(description=runHelp, formatter_class=argparse.RawTextHelpFormatter)
    # required
    parser.add_argument('-u', '--username', nargs='?', dest='username', type=str,
                        help='User name for your SkyTap account.', required=True)
    parser.add_argument('-t', '--token', nargs='?', dest='token', type=str,
                        help='The API Security Token to access your SkyTap account', required=True)
    # optional
    parser.add_argument('-e', '--environment', nargs='*', dest='envs', type=int,
                        help='The environment ID(s) to operate against')
    # optional, default is list
    commandlist = ', '.join(sorted(commandtable.keys()))
    parser.add_argument('-c', '--command', nargs='?', dest='command', type=str,
                        help='Commands are: '+ commandlist, default='list')
    # workaround for problem with help from argparse
    parser._optionals.title = 'flag arguments'
    return parser


def main():
    # get arguments
    parser = setupArgParser()
    args = parser.parse_args()

    # Validate command
    lcCommand = str.lower(args.command)
    if lcCommand not in ['list', 'start', 'suspend', 'resume', 'stop', 'halt', 'restart']:
        sys.exit("Unknown command\n")

    # if a list of environments is not passed in, get a list of all environments
    envs = []
    if args.envs == None:
        allEnvs = getEnvs(args)
        for someEnv in allEnvs:
            someEnvId = someEnv['id']
            envs.append(someEnvId)
    else:
        envs = args.envs

    # envs list populated now
    for someEnvIdInt in envs:
        someEnvId = '{0}'.format(someEnvIdInt)
        someEnv = getEnv(args, someEnvId)
        # Default command is list, and list is used at start of all commands
        reportEnv(someEnv)
        allVMs = someEnv['vms']
        reportVMs(allVMs)
        vmsForRunChange = []
        commandmap = commandtable[lcCommand]
        if commandmap != None:
            newVMRunstate = commandmap['newrunstate']
            targetVMRunstate = commandmap['finalrunstate']
            acceptRunstates = commandmap['acceptrunstates']
            runmult = commandmap['runmulti']
            for someVM in allVMs:
                # Get the currents state of the VM
                vmRunstate = someVM['runstate']
                # Handle commands
                if vmRunstate in  acceptRunstates:
                    vmsForRunChange.append(someVM)
            if len(vmsForRunChange) > 0:
                if runmult:
                    setVMMultiListRunstate(args, someEnvId, vmsForRunChange, newVMRunstate, targetVMRunstate)
                else:
                    setVMListRunstate(args, vmsForRunChange, newVMRunstate, targetVMRunstate)
                for someVM in vmsForRunChange:
                    refreshVM = getVM(args, someVM['id'])
                    reportVM(refreshVM)


if __name__ == '__main__':
    main()
