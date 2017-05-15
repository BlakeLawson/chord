%Script to plot latency of data.
% store current directory and change directory to data directory
currDir = cd;
cd('../data/')

latency_offset = 2;
first = csvread('distinct-lookup-out10.csv', 0, 3); 
second = csvread('distinct-lookup-out30.csv',0,3);

l1 = first(:,latency_offset);
l2 = second(:,latency_offset);

% multiply values by 1000 to convert from s to ms
latency = [l1 l2] *1000;
x = [10 30];
[rows, cols] = size(latency);

all_mean = zeros(1,cols);
all_p1 = zeros(1, cols);
all_p99 = zeros(1, cols);

for idx = 1:cols
    [m, p1, p99] = get_statistics(latency(:,idx));
    all_mean(idx) = m;
    all_p1(idx) = p1;
    all_p99(idx) = p99;
end

p1_dist = abs(all_mean - all_p1);
p99_dist = abs(all_mean - all_p99);

figure(2)
errorbar(x, all_mean, p1_dist, p99_dist,'vertical', 'xk')
axis([min(x)-10 max(x)+10 min(all_p1)-50 max(all_p99)+50])
title('Iterative Lookups between Data Centers')
xlabel('Number of Nodes')
ylabel('Latency (s)')


cd(currDir)
