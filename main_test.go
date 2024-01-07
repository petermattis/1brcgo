package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

func (m *robinHoodMap) Count() int {
	var n int
	m.Iterate(func(_ string, _ *measurement) {
		n++
	})
	return n
}

func TestParseTemp(t *testing.T) {
	// Temperature parsing is manually inlined in the process() function. This
	// adapter allows testing of only the temperature parsing.
	parseTemp := func(s string) int64 {
		m := newRobinHoodMap(1)
		process([]byte("a;"+s+"\n"), m)
		var temp int64
		m.Iterate(func(_ string, m *measurement) {
			temp = m.max
		})
		return temp
	}

	for v := -99.9; v <= 99.9; v += 0.1 {
		d := fmt.Sprintf("%0.1f", v)
		n := parseTemp(d)
		f := fmt.Sprintf("%0.1f", float64(n)/10.0)
		if d != f && n != 0 {
			t.Fatalf("expected %s, got %s", d, f)
		}
	}
}

func TestRobinHood(t *testing.T) {
	m := newRobinHoodMap(1)
	for i := 0; i < 100; i++ {
		m.Upsert("key"+fmt.Sprint(i), func(v *measurement) {
			v.count = int64(i + 1)
		})
		if m.Count() != i+1 {
			t.Fatalf("%d != %d", m.Count(), i+1)
		}

		for j := 0; j <= i; j++ {
			m.Upsert("key"+fmt.Sprint(j), func(v *measurement) {
				if v.count != int64(j+1) {
					t.Fatalf("%d: %d != %d", j, v.count, j+1)
				}
			})
		}
	}
}

func BenchmarkProcess(b *testing.B) {
	data, indexes := func() ([]byte, []int) {
		var stations = []string{
			"Abha", "Abidjan", "Abéché", "Accra", "Addis Ababa", "Adelaide", "Aden", "Ahvaz",
			"Albuquerque", "Alexandra", "Alexandria", "Algiers", "Alice Springs", "Almaty", "Amsterdam",
			"Anadyr", "Anchorage", "Andorra la Vella", "Ankara", "Antananarivo", "Antsiranana", "Arkhangelsk",
			"Ashgabat", "Asmara", "Assab", "Astana", "Athens", "Atlanta", "Auckland", "Austin",
			"Baghdad", "Baguio", "Baku", "Baltimore", "Bamako", "Bangkok", "Bangui", "Banjul",
			"Barcelona", "Bata", "Batumi", "Beijing", "Beirut", "Belgrade", "Belize City", "Benghazi",
			"Bergen", "Berlin", "Bilbao", "Birao", "Bishkek", "Bissau", "Blantyre", "Bloemfontein",
			"Boise", "Bordeaux", "Bosaso", "Boston", "Bouaké", "Bratislava", "Brazzaville", "Bridgetown",
			"Brisbane", "Brussels", "Bucharest", "Budapest", "Bujumbura", "Bulawayo", "Burnie", "Busan",
			"Cabo San Lucas", "Cairns", "Cairo", "Calgary", "Canberra", "Cape Town", "Changsha", "Charlotte",
			"Chiang Mai", "Chicago", "Chihuahua", "Chișinău", "Chittagong", "Chongqing", "Christchurch", "City of San Marino",
			"Colombo", "Columbus", "Conakry", "Copenhagen", "Cotonou", "Cracow", "Da Lat", "Da Nang",
			"Dakar", "Dallas", "Damascus", "Dampier", "Dar es Salaam", "Darwin", "Denpasar", "Denver",
			"Detroit", "Dhaka", "Dikson", "Dili", "Djibouti", "Dodoma", "Dolisie", "Douala",
			"Dubai", "Dublin", "Dunedin", "Durban", "Dushanbe", "Edinburgh", "Edmonton", "El Paso",
			"Entebbe", "Erbil", "Erzurum", "Fairbanks", "Fianarantsoa", "Flores", "Frankfurt", "Fresno",
			"Fukuoka", "Gabès", "Gaborone", "Gagnoa", "Gangtok", "Garissa", "Garoua", "George Town",
			"Ghanzi", "Gjoa Haven", "Guadalajara", "Guangzhou", "Guatemala City", "Halifax", "Hamburg", "Hamilton",
			"Hanga Roa", "Hanoi", "Harare", "Harbin", "Hargeisa", "Hat Yai", "Havana", "Helsinki",
			"Heraklion", "Hiroshima", "Ho Chi Minh City", "Hobart", "Hong Kong", "Honiara", "Honolulu", "Houston",
			"Ifrane", "Indianapolis", "Iqaluit", "Irkutsk", "Istanbul", "İzmir", "Jacksonville", "Jakarta",
			"Jayapura", "Jerusalem", "Johannesburg", "Jos", "Juba", "Kabul", "Kampala", "Kandi",
			"Kankan", "Kano", "Kansas City", "Karachi", "Karonga", "Kathmandu", "Khartoum", "Kingston",
			"Kinshasa", "Kolkata", "Kuala Lumpur", "Kumasi", "Kunming", "Kuopio", "Kuwait City", "Kyiv",
			"Kyoto", "La Ceiba", "La Paz", "Lagos", "Lahore", "Lake Havasu City", "Lake Tekapo", "Las Palmas de Gran Canaria",
			"Las Vegas", "Launceston", "Lhasa", "Libreville", "Lisbon", "Livingstone", "Ljubljana", "Lodwar",
			"Lomé", "London", "Los Angeles", "Louisville", "Luanda", "Lubumbashi", "Lusaka", "Luxembourg City",
			"Lviv", "Lyon", "Madrid", "Mahajanga", "Makassar", "Makurdi", "Malabo", "Malé",
			"Managua", "Manama", "Mandalay", "Mango", "Manila", "Maputo", "Marrakesh", "Marseille",
			"Maun", "Medan", "Mek'ele", "Melbourne", "Memphis", "Mexicali", "Mexico City", "Miami",
			"Milan", "Milwaukee", "Minneapolis", "Minsk", "Mogadishu", "Mombasa", "Monaco", "Moncton",
			"Monterrey", "Montreal", "Moscow", "Mumbai", "Murmansk", "Muscat", "Mzuzu", "N'Djamena",
			"Naha", "Nairobi", "Nakhon Ratchasima", "Napier", "Napoli", "Nashville", "Nassau", "Ndola",
			"New Delhi", "New Orleans", "New York City", "Ngaoundéré", "Niamey", "Nicosia", "Niigata", "Nouadhibou",
			"Nouakchott", "Novosibirsk", "Nuuk", "Odesa", "Odienné", "Oklahoma City", "Omaha", "Oranjestad",
			"Oslo", "Ottawa", "Ouagadougou", "Ouahigouya", "Ouarzazate", "Oulu", "Palembang", "Palermo",
			"Palm Springs", "Palmerston North", "Panama City", "Parakou", "Paris", "Perth", "Petropavlovsk-Kamchatsky", "Philadelphia",
			"Phnom Penh", "Phoenix", "Pittsburgh", "Podgorica", "Pointe-Noire", "Pontianak", "Port Moresby", "Port Sudan",
			"Port Vila", "Port-Gentil", "Portland (OR)", "Porto", "Prague", "Praia", "Pretoria", "Pyongyang",
			"Rabat", "Rangpur", "Reggane", "Reykjavík", "Riga", "Riyadh", "Rome", "Roseau",
			"Rostov-on-Don", "Sacramento", "Saint Petersburg", "Saint-Pierre", "Salt Lake City", "San Antonio", "San Diego", "San Francisco",
			"San Jose", "San José", "San Juan", "San Salvador", "Sana'a", "Santo Domingo", "Sapporo", "Sarajevo",
			"Saskatoon", "Seattle", "Ségou", "Seoul", "Seville", "Shanghai", "Singapore", "Skopje",
			"Sochi", "Sofia", "Sokoto", "Split", "St. John's", "St. Louis", "Stockholm", "Surabaya",
			"Suva", "Suwałki", "Sydney", "Tabora", "Tabriz", "Taipei", "Tallinn", "Tamale",
			"Tamanrasset", "Tampa", "Tashkent", "Tauranga", "Tbilisi", "Tegucigalpa", "Tehran", "Tel Aviv",
			"Thessaloniki", "Thiès", "Tijuana", "Timbuktu", "Tirana", "Toamasina", "Tokyo", "Toliara",
			"Toluca", "Toronto", "Tripoli", "Tromsø", "Tucson", "Tunis", "Ulaanbaatar", "Upington",
			"Ürümqi", "Vaduz", "Valencia", "Valletta", "Vancouver", "Veracruz", "Vienna", "Vientiane",
			"Villahermosa", "Vilnius", "Virginia Beach", "Vladivostok", "Warsaw", "Washington", "Wau", "Wellington",
			"Whitehorse", "Wichita", "Willemstad", "Winnipeg", "Wrocław", "Xi'an", "Yakutsk", "Yangon",
			"Yaoundé", "Yellowknife", "Yerevan", "Yinchuan", "Zagreb", "Zanzibar City", "Zürich",
		}

		var buf bytes.Buffer
		var indexes []int
		for i := 0; i < 10000; i++ {
			fmt.Fprintf(&buf, "%s;%0.1f\n", stations[rand.Intn(len(stations))], 99.9*(rand.Float64()*2-1))
			indexes = append(indexes, buf.Len())
		}
		return buf.Bytes(), indexes
	}()
	cities := newRobinHoodMap(5000)

	b.ResetTimer()
	for i := 0; i < b.N; i += len(indexes) {
		n := len(indexes)
		if i+n > b.N {
			n = b.N - i
		}
		process(data[:indexes[n-1]], cities)
	}
}
