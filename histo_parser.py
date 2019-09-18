#!/usr/bin/env python3.7

import argparse
import re

def openAWRFile(fn=None):
    with open(fn, 'r') as awr_fh:
        awrf = awr_fh.readlines()
    return awrf


def get_histogram(awr_file=None, histo_type=None, wait_event=None):
    try:
        awr = openAWRFile(fn=awr_file)
    except FileNotFoundError as e:
        print('Error: ' + e.filename + ' not found.')

    # -- line_tracking (lt)
    # lt will be used to tell the parser if it is in the section of the text file that contains the histogram
    # that we are looking for.
    # Phase 1 = True, means the parser found the histogram topic
    # Phase 2 = True, means the parser found the histogram buckets
    # if Phase 1, Phase 2, and a match on the wait event is achieved, then we load the dictionary with all the buckets
    # values.

    lt = {}
    lt['phase1'] = False
    lt['phase2'] = False

    histo = {}
    for awr_line in awr:
        if re.match(histo_type, awr_line) or lt['phase1']:
            lt['phase1'] = True
            if lt['phase1'] and re.match('Event', awr_line):
                lt['phase2'] = True
                line = awr_line.split()

                if histo_type == 'Wait Event Histogram':
                    if line[0] == 'Event' and line[1] == 'Waits':
                        for i in line:
                            if i not in histo.keys():
                                histo[i] = ''
                        continue
                    else:
                        lt['phase2'] = False

                if histo_type == 'Wait Event Histogram \(up to 32 ms\)':
                    if line[0] == 'Event' and line[2] == '32m':
                        for i in line:
                            if i not in histo.keys() and i != 'to':
                                histo[i] = ''
                        continue
                    else:
                        lt['phase2'] = False

            if lt['phase1'] and lt['phase2'] and re.match(wait_event, awr_line):
                # Converting the number of event
                num_events = awr_line[26:32].strip()
                if num_events[-1:] == 'K':
                    num_events = float(num_events[:-1]) * 1000
                elif num_events[-1:] == 'M':
                    num_events = float(num_events[:-1]) * 1000000
                elif num_events[-1:] == 'G':
                    num_events = float(num_events[:-1]) * 1000000000

                if histo_type == 'Wait Event Histogram':
                    histo['Event'] = awr_line[:25].strip()
                    histo['Waits'] = str(num_events)
                    histo['<8us'] = awr_line[33:38].strip()
                    histo['<16us'] = awr_line[39:44].strip()
                    histo['<32us'] = awr_line[45:50].strip()
                    histo['<64us'] = awr_line[51:56].strip()
                    histo['<128u'] = awr_line[57:62].strip()
                    histo['<256u'] = awr_line[63:68].strip()
                    histo['<512u'] = awr_line[69:74].strip()
                    histo['>=512'] = awr_line[75:80].strip()
                    lt['phase1'] = False
                    lt['phase2'] = False
                    continue
                elif histo_type == 'Wait Event Histogram \(up to 32 ms\)':
                    histo['Event'] = awr_line[:25].strip()
                    histo['32m'] = str(num_events)
                    histo['<512'] = awr_line[33:38].strip()
                    histo['<1ms'] = awr_line[39:44].strip()
                    histo['<2ms'] = awr_line[45:50].strip()
                    histo['<4ms'] = awr_line[51:56].strip()
                    histo['<8ms'] = awr_line[57:62].strip()
                    histo['<16ms'] = awr_line[63:68].strip()
                    histo['<32ms'] = awr_line[69:74].strip()
                    histo['>=32m'] = awr_line[75:80].strip()
                    lt['phase1'] = False
                    lt['phase2'] = False
                    continue
        else:
            lt['phase1'] = False
    return histo


def getCmdArgs():
    oap_args = argparse.ArgumentParser(description='Oracle AWR Parser')
    oap_args.add_argument('--get-histo', choices=['total_waits', 'up_to_32ms'], help='Returns Wait Event Histograms')
    oap_args.add_argument('--awr', nargs='+', dest='awr_files_list', required=True, help='List of AWR files to be parsed')
    return oap_args.parse_args()


def main():
    cli_args = getCmdArgs()

    wait_event_list = [
        'db file sequential read',
        'log file parallel write'
    ]

    #
    # -- Note: There are two possible options for histo_type:
    # (total_waits) Wait Event Histogram
    # (up_to_32ms ) Wait Event Histogram (up to 32 ms),
    #
    # While specifying (up_to_32ms) you have to add a pair of backslash before each parentheses, otherwise the re.match
    # method won't be able to match the variable with the string in the awr.txt file.
    # For example: histo_type='Wait Event Histogram \\(up to 32 ms\\)'
    #

    if cli_args.get_histo:
        header_added = False
        output_fn = 'histogram_' + cli_args.get_histo + '_summary.csv'
        with open(output_fn, 'w') as out_fd:
            for awr_file in cli_args.awr_files_list:
                for event in wait_event_list:
                    if cli_args.get_histo == 'total_waits':
                        histogram = get_histogram(awr_file=awr_file, histo_type='Wait Event Histogram',
                                                  wait_event=event)
                    elif cli_args.get_histo == 'up_to_32ms':
                        histogram = get_histogram(awr_file=awr_file, histo_type='Wait Event Histogram \\(up to 32 ms\\)',
                                                  wait_event=event)
                    if not header_added:

                        header = ','.join(histogram.keys())
                        header += ',Filename'
                        header_added = True
                        out_fd.write(header + '\n')
                    output_line = ','.join(histogram.values())
                    output_line += ',' + awr_file
                    out_fd.write(output_line + '\n')


if __name__ == '__main__':
    main()