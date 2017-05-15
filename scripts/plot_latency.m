%Script to plot latency of data.
% store current directory and change directory to data directory
currDir = cd;
cd('../data/')

% read in files
offset = 2; %set offset to read latency data
const = 1000;
first = csvread('recursive-lookup-out10.csv', 0, 3)*const; 
second = csvread('recursive-lookup-out30.csv',0, 3)*const;
third = csvread('recursive-lookup-out60.csv', 0, 3)*const; 
fourth = csvread('recursive-lookup-out90.csv',0, 3)*const;
fifth = csvread('recursive-lookup-out120.csv', 0, 3)*const; 
sixth = csvread('recursive-lookup-out150.csv',0, 3)*const;
seventh = csvread('recursive-lookup-out180.csv',0, 3)*const;
eight = csvread('recursive-lookup-out200.csv',0, 3)*const;

hops = [first(:,offset), second(:,offset), third(:,offset), fourth(:,offset),...
    fifth(:,offset), sixth(:,offset), seventh(:,offset), eight(:,offset)];
[rows, col] = size(hops);
x = [10 30 60 90 120 150 180 200];

all_mean = zeros(1,col);
all_p1 = zeros(1, col);
all_p99 = zeros(1, col);

for idx = 1:col
    [m, p1, p99] = get_statistics(hops(:,idx));
    all_mean(idx) = m;
    all_p1(idx) = p1;
    all_p99(idx) = p99;
end

p1_dist = abs(all_mean - all_p1);
p99_dist = abs(all_mean - all_p99);

figure(2)
errorbar(x, all_mean, p1_dist, p99_dist,'vertical', 'xk')
axis([min(x)-10 max(x)+10 min(all_p1)-50 max(all_p99)+50])
title('Recursive Lookups in a single Data Centers')
xlabel('Number of Nodes')
ylabel('Latency(ms)')

cd(currDir)
