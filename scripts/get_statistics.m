function [m, p1, p99] = get_statistics(input)
    m = mean(input);
    p1 = prctile(input, 1);
    p99 = prctile(input, 99);