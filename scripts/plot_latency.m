%Script to plot path lengths of data.
% store current directory and change directory to data directory
currDir = cd;
cd('../data/')

path_length_offset = 1;
latency_offset = 2;
first = csvread('distinct-lookup-out10.csv', 0, 3);
second = csvread('distinct-lookup-out30.csv',0,3);

look_up_first = first(:,path_length_offset);
look_up_second = second(:,path_length_offset);

latency_first = first(:,latency_offset);
latency_second = second(:,latency_offset);

[f_mean, f_1, f_99] = get_statistics(latency_first);

[s_mean, s_1, s_99] = get_statistics(latency_second);

f_mean = f_mean * 1000;
f_1 = f_1 * 1000;
f_99 = f_99 * 1000;
s_mean = s_mean * 1000;
s_99 = s_99 * 1000;

all_mean = [f_mean s_mean];
p1 = abs([f_1 s_1] - all_mean);
p99 = abs([f_99 s_99] - all_mean);
x = [10 30];

figure(2)
errorbar(x, all_mean, p1, p99,'vertical', 'xk')
axis([5 35 -25 700])
title('Latency vs. Number of Nodes (Iterative lookups)')
xlabel('Number of Nodes')
ylabel('Latency (s)')


cd(currDir)
