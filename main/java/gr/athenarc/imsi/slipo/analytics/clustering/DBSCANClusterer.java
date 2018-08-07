package gr.athenarc.imsi.slipo.analytics.clustering;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import gr.athenarc.imsi.slipo.analytics.Grid;
import gr.athenarc.imsi.slipo.analytics.POI;

public class DBSCANClusterer{

	protected double eps;
	protected int minPts;

	public DBSCANClusterer(final double eps, final int minPts) {
		this.eps = eps;
		this.minPts = minPts;
	}


	public List<List<POI>> cluster(List<POI> pois) {

		Grid grid = new Grid(pois, eps);
		List<List<POI>> clusters = new ArrayList<List<POI>>();

		for (POI poi : pois) {
			if (poi.getAttributes().get("dbscan_status") != null) {
				continue;
			}
			Set<POI> neighbors = new HashSet<POI>(grid.getNeighbors(poi));
			if (neighbors.size() >= minPts) {
				List<POI> cluster = new ArrayList<POI>();
				clusters.add(expandCluster(cluster, poi, neighbors, pois, grid));
			} else {
				poi.getAttributes().put("dbscan_status", "NOISE");
			}
		}
		return clusters;
	}

	private List<POI> expandCluster(List<POI> cluster, POI poi, Set<POI> seeds, List<POI> pois, Grid grid) {
		cluster.add(poi);
		poi.getAttributes().put("dbscan_status", "PART_OF_CLUSTER");

		//Injection Here!
		poi.getAttributes().put("density_status", "DENSE");


		while (seeds.size() > 0) {
			POI current = seeds.iterator().next();
			seeds.remove(current);
			if (current.getAttributes().get("dbscan_status") == null) {
				List<POI> currentNeighbors = grid.getNeighbors(current);
				if (currentNeighbors.size() >= minPts) {
					seeds.addAll(currentNeighbors);
					current.getAttributes().put("density_status", "DENSE");
				}
				else{
					current.getAttributes().put("density_status", "NOT_DENSE");
				}
			}

			if (current.getAttributes().get("dbscan_status") != "PART_OF_CLUSTER") {
				current.getAttributes().put("dbscan_status", "PART_OF_CLUSTER");
				cluster.add(current);
			}
		}
		return cluster;
	}
}

