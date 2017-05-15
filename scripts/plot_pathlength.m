%%Script to plot path lengths of data.
% store current directory and change directory to data directory
currDir = cd;
cd('../data/')

% read in files
path_length_offset = 1;
first = csvread('distinct-lookup-out10.csv', 0, 3); 
second = csvread('distinct-lookup-out30.csv',0, 3);

p1 = first(:,path_length_offset);
p2 = second(:,path_length_offset);

hops = [p1 p2];
x = [10 30];
[rows, cols] = size(hops);

all_mean = zeros(1,cols);
all_p1 = zeros(1, cols);
all_p99 = zeros(1, cols);

for idx = 1:cols
    [m, p1, p99] = get_statistics(hops(:,idx));
    all_mean(idx) = m;
    all_p1(idx) = p1;
    all_p99(idx) = p99;
end

p1_dist = abs(all_mean - all_p1);
p99_dist = abs(all_mean - all_p99);

figure(1)
errorbar(x, all_mean, p1_dist, p99_dist,'vertical', 'xk')
axis([min(x)-5 max(x)+5 min(all_p1)-0.5 max(all_p99)+0.5])
title('Iterative Lookups between Data Centers')
xlabel('Number of Nodes')
ylabel('Latency (ms)')


cd(currDir)
