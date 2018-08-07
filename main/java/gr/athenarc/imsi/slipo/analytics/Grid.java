package gr.athenarc.imsi.slipo.analytics;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Grid {
	private Map<Integer, Map<Integer, List<POI>>> cells;
	private double offsetX, offsetY, eps;

	public Grid(List<POI> pois, double eps) {
		this.eps = eps;
		cells = new HashMap<Integer, Map<Integer, List<POI>>>();
		int i, j;
		offsetX = pois.get(0).getPoint().getX();
		offsetY = pois.get(0).getPoint().getY();
		for (POI poi : pois) {
			i = (int) Math.floor((poi.getPoint().getX() - offsetX) / eps);
			j = (int) Math.floor((poi.getPoint().getY() - offsetY) / eps);
			if (cells.get(i) == null) {
				cells.put(i, new HashMap<Integer, List<POI>>());
			}
			if (cells.get(i).get(j) == null) {
				cells.get(i).put(j, new ArrayList<POI>());
			}
			cells.get(i).get(j).add(poi);
		}
	}

	public List<POI> getNeighbors(POI poi) {
		List<POI> neighbors = new ArrayList<POI>();
		int i = (int) Math.floor((poi.getPoint().getX() - offsetX) / eps);
		int j = (int) Math.floor((poi.getPoint().getY() - offsetY) / eps);
		for (int i1 = i - 1; i1 <= i + 1; i1++) {
			for (int j1 = j - 1; j1 <= j + 1; j1++) {
				if (cells.get(i1) != null && cells.get(i1).get(j1) != null) {
					for (POI neighbor : cells.get(i1).get(j1)) {
						if (!poi.getId().equals(neighbor.getId())
								&& Math.abs(poi.getPoint().getX() - neighbor.getPoint().getX()) <= eps
								&& Math.abs(poi.getPoint().getY() - neighbor.getPoint().getY()) <= eps) {
							neighbors.add(neighbor);
						}
					}
				}
			}
		}
		return neighbors;
	}

	public Map<Integer, Map<Integer, List<POI>>> getCells() {
		return cells;
	}

	public double getOffsetX() {
		return offsetX;
	}

	public double getOffsetY() {
		return offsetY;
	}

	public double getEps() {
		return eps;
	}

	public Polygon cellIndexToGeometry(Integer row, Integer column, GeometryFactory geometryFactory) {
		double lon = row * eps + offsetX;
		double lat = column * eps + offsetY;
		Coordinate ll = new Coordinate(lon, lat);
		Coordinate lr = new Coordinate(lon + eps, lat);
		Coordinate ur = new Coordinate(lon + eps, lat + eps);
		Coordinate ul = new Coordinate(lon, lat + eps);
		Coordinate[] coords = new Coordinate[] { ll, lr, ur, ul, ll };
		Polygon border = geometryFactory.createPolygon(coords);
		return border;
	}
}