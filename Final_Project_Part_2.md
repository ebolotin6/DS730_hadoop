# Final Project - Part 2

### Parallel programming in Java: "Mailman's dilemma" algorithm

#### Instructions

The solution to this problem must be done using Java threads. Due to budget cutbacks, the postal services at UW-Oshkosh can only afford 1 mail deliverer. Even worse, that deliverer is a student who works part-time. Postal services wants to minimize the amount of time that student has to work in order to save money. Because of this, they are interested in the fastest way to visit all buildings and return back to the Campus Services Building, BldgOne in the example below. There are obvious routes that are terrible (e.g. going from one side of campus to the other and then back) but the optimal route is not obvious. Your goal is to read in a file that gives the time in seconds to get from a building to every other building and determine the best possible route such that you start at the building listed on the first line, visit all other buildings and end at the building listed on the first line. The building names in the example below are arbitrary and can be called anything. The input file you will read in is called input2.txt​ and will be formatted in the following manner:

```
BldgOne : t(BldgOne) t(BldgTwo) t(BldgThree) t(BldgFour) t(BldgFive) 
BldgTwo : t(BldgOne) t(BldgTwo) t(BldgThree) t(BldgFour) t(BldgFive) 
BldgThree : t(BldgOne) t(BldgTwo) t(BldgThree) t(BldgFour) t(BldgFive) 
BldgFour : t(BldgOne) t(BldgTwo) t(BldgThree) t(BldgFour) t(BldgFive)
BldgFive : t(BldgOne) t(BldgTwo) t(BldgThree) t(BldgFour) t(BldgFive)
```

Take the first line for example. t(BldgTwo) will be an integer value denoting the number of seconds it takes to get from BldgOne to BldgTwo. On the first line, t(BldgOne) will be 0. In other words, it takes 0 time to get from BldgOne to BldgOne. The input will always be formatted in this manner. If another building is constructed, it will be added to the end and the file will be updated accordingly. For example, if BldgSix were constructed, the time to BldgSix will be added at the end of every list and BldgSix will be added to the end of the file. The time from BldgOne to BldgThree may not be the same as the time from BldgThree to BldgOne. There may be one way streets; it may be uphill, etc. A sample input file is shown below:

```
BldgA : 0 5 7
BldgB : 4 0 3
BldgC : 6 4 0
```

___Your goal is this___: for the best route possible, print out the total time taken to start with the building on the first line, visit all buildings and then return to the building on the first line. You must also output the order in which you visited the buildings using the name of the buildings as defined in the input file. In the above example, there are only 2 possible routes: BldgA -> BldgB -> BldgC -> BldgA which is 5 + 3 + 6 == 14. The other route is: BldgA -> BldgC -> BldgB -> BldgA which is 7 + 4 + 4 == 15. Therefore, the output would be:

```
14 BldgA BldgB BldgC
```

#### Solution in Python
---

```python
#!/usr/bin/env python3

import sys
from itertools import permutations
import timeit

start_time = timeit.default_timer()

buildings_dict = {}
building_id_map_list = []
building_id_map_dict = {}

# read in file and create list of buildings and dict of buildings + times
with open(sys.argv[1],"r") as file:
    for i, line in enumerate(file):
        line = line.split(" : ")
        building = line[0]
        travel_times = line[1].strip("\n").split(" ")
        buildings_dict[i] = list(map(int, travel_times))
        building_id_map_list.append((i, building))
        building_id_map_dict[i] = building

# generate permutations of routes
perm_list = list(permutations([i for i, name in building_id_map_list[1:]]))
all_routes = []
for i in range(len(perm_list)):
    perm = list(perm_list[i])
    perm.insert(0,building_id_map_list[0][0])
    perm.append(building_id_map_list[0][0])
    all_routes.append(perm)

# create new list to contain route times
route_times = []

# loop through every route (permutation) in all_routes and set total_time to 0
for i, route in enumerate(all_routes):
    total_time = 0

    # loop through sequence of indices for every route and define current + next building
    for j in range(1, len(route)):
        current_building = route[j-1]
        next_building = route[j]

        # get time to go from current building to next building
        time_to_next_bld = buildings_dict[current_building][next_building]
        total_time += time_to_next_bld
    
    # append route index and time to route_times
    route_times.append((i, total_time))


# get minimum travel time
min_time = min(route_times, key = lambda route: route[1])

# get route
route = ' '.join([str(building_id_map_dict[i]) for i in all_routes[min_time[0]][:-1]])

# subset route time
time = min_time[1]

print(f"{time} {route}")

stop_time = timeit.default_timer()

print('Time elapsed: ', stop_time - start_time)
```

#### Solution in Java
---

```java
import java.util.*;
import java.io.*;
public class RouteOptimizer extends Thread {
    // private ListIterator<List<Integer>> iterator;
    private List<List<Integer>> perm_subset;
    private TreeMap<Integer, ArrayList<Integer>> buildings_map;
    private TreeMap<Integer, Integer> route_times;
    private int start;
    
    public RouteOptimizer(List<List<Integer>> perm_subset, TreeMap<Integer, ArrayList<Integer>> buildings_map, int start) throws Exception {
        this.buildings_map = buildings_map;
        this.route_times = new TreeMap<>();
        this.start = start;
        this.perm_subset = perm_subset;
    }

    public void run() {
        // for each route in an array, compute the time of the route and assign the route an id. add this to route_times array.

        int i = start;
        for (List<Integer> route : perm_subset) {
            int total_time = 0;

            for (int j = 1; j < route.size(); j++) {
                int current_building_index = route.get(j-1);
                int next_building_index = route.get(j);
                ArrayList<Integer> building_times = buildings_map.get(current_building_index);
                int time_to_next_bld = building_times.get(next_building_index);
                total_time += time_to_next_bld;
            }

            route_times.put(i, total_time);
            i += 1;
        }
    }

    public TreeMap<Integer, Integer> get_route_times() {
        return route_times;
    }

    // function to generate permutations
    public static List<List<Integer>> get_permutations(List<Integer> original) {
        if (original.size() == 0) {
            List<List<Integer>> result = new ArrayList<List<Integer>>(); 
            result.add(new ArrayList<Integer>()); 
            return result; 
        }

        Integer firstElement = original.remove(0);
        List<List<Integer>> returnValue = new ArrayList<List<Integer>>();
        List<List<Integer>> permutations = get_permutations(original);

        for (List<Integer> smallerPermutated : permutations) {
            for (int index=0; index <= smallerPermutated.size(); index++) {
                List<Integer> temp = new ArrayList<Integer>(smallerPermutated);
                temp.add(index, firstElement);
                returnValue.add(temp);
            }
        }
        return returnValue;
    }

    public static void main(String[] args) throws Exception {
        // Start timer
        long startTime = System.currentTimeMillis();
        
        TreeMap<Integer, ArrayList<Integer>> buildings_map = new TreeMap<>();
        TreeMap<Integer, String> buildings_id_map = new TreeMap<>();
        List<Integer> input_list = new ArrayList<>();

        // Read in file
        File file = new File("input2.txt");

        // read in file
        try(BufferedReader br = new BufferedReader(new FileReader(file))) {
            {
                int building_id = 0;
                for(String line;  (line = br.readLine()) != null; building_id++ ) {
                    // split file into list by colon
                    ArrayList<String> line_list = new ArrayList<String>(Arrays.asList(line.split(" : ")));
                    String building_name = line_list.get(0);

                    // convert (string) travel times to list
                    ArrayList<String> travel_times_str = new ArrayList<String>(Arrays.asList(line_list.get(1).replace("\n", "").split(" ")));
                    ArrayList<Integer> travel_times = new ArrayList<Integer>();
                    
                    // convert travel times to int
                    for(String time : travel_times_str) travel_times.add(Integer.valueOf(time));
                    
                    // write building key + travel times to TreeMap
                    buildings_map.put(building_id, travel_times);
                    buildings_id_map.put(building_id, building_name);
                    if(building_id != 0) {
                        input_list.add(building_id);
                    }
                }
            }

        }

        // get permutations
        List<List<Integer>> permutations = get_permutations(input_list);

        // loop through permutations and add start/end building
        for (int i = 0; i < permutations.size(); i++) {
            List<Integer> perm = permutations.get(i);
            perm.add(0,0);
            perm.add(0);
            permutations.set(i, perm);
        }

        // section start: split the list of permutations into chunks, and assign each chunk to a thread for mapping route to time
        int num_permutations = permutations.size();
        int num_threads = 1;

        if(num_permutations >= 100) {
            num_threads = 20;
        }
        
        RouteOptimizer[] agents = new RouteOptimizer[num_threads];
        
        int count = 1;
        int start = 0;
        int perm_per_thread = num_permutations / num_threads;
        int thread_id = 0;
        int len_test = 0;

        for (int i = 0; i < num_permutations; i++) {
            if(count == perm_per_thread) {
                if((i + 1) == num_permutations) i += 1;
                List<List<Integer>> perm_subset = permutations.subList(start, i);
                agents[thread_id] = new RouteOptimizer(perm_subset, buildings_map, start);
                agents[thread_id].start();
                start = i;
                count = 0;
                thread_id += 1;
            }
            count += 1;
        }
        // section end

        TreeMap<Integer, Integer> all_routes = new TreeMap<>();

        // section start: get mapped list of route times for each thread
        for(RouteOptimizer agent : agents) {
            if(agent.isAlive()) {
                agent.join();
            }

            TreeMap<Integer, Integer> thread_routes = agent.get_route_times();

            for (Map.Entry<Integer, Integer> entry : thread_routes.entrySet()) {
                int i = entry.getKey();
                int time = entry.getValue();
                all_routes.put(i, time);
            }
        }

        // get minimum route time
        Integer min_time = all_routes.values().stream().min(Integer::compare).get();

        // get route for minimum route time
        for (Map.Entry<Integer, Integer> entry : all_routes.entrySet()) {
            if (entry.getValue().equals(min_time)) {
                FileWriter fileWriter = new FileWriter("output2.txt");
                PrintWriter output = new PrintWriter(fileWriter);

                Integer min_index = entry.getKey();             
                List<Integer> route_indices = permutations.get(min_index);
                String optimal_route = Integer.toString(min_time);
                
                // print results
                for (int i = 0; i < route_indices.size()-1; i++) {
                    String next_building = buildings_id_map.get(route_indices.get(i));
                    optimal_route = optimal_route + " " + next_building;
                }
                output.print(optimal_route);
                output.close();
                break;
            }
        }       

        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        System.out.println(elapsedTime);
    }   
}
```
