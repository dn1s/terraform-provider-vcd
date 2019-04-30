package vcd

import (
	"testing"

	"github.com/hashicorp/terraform/helper/resource"
	"github.com/vmware/go-vcloud-director/v2/govcd"
)

func TestAccVcdVAppVmMultiNIC(t *testing.T) {
	var (
		vapp        govcd.VApp
		vm          govcd.VM
		netVappName string = t.Name()
		netVmName1  string = t.Name() + "VM"
	)

	if vcdShortTest {
		t.Skip(acceptanceTestsSkipped)
		return
	}
	var params = StringMap{
		"Org":         testConfig.VCD.Org,
		"Vdc":         testConfig.VCD.Vdc,
		"EdgeGateway": testConfig.Networking.EdgeGateway,
		"Catalog":     testSuiteCatalogName,
		"CatalogItem": testSuiteCatalogOVAItem,
		"VAppName":    netVappName,
		"VMName":      netVmName1,
	}

	configTextVM := templateFill(testAccCheckVcdVAppVmNetwork, params)

	debugPrintf("#[DEBUG] CONFIGURATION: %s\n", configTextVM)
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckVcdVAppVmDestroy(netVappName),
		Steps: []resource.TestStep{
			resource.TestStep{
				Config: configTextVM,
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckVcdVAppVmExists(netVappName, netVmName1, "vcd_vapp_vm."+netVmName1, &vapp, &vm),
					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "name", netVmName1),

					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.0.orgnetwork", "multinic-net"),
					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.0.is_primary", "false"),
					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.0.ip_allocation_mode", "POOL"),
					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.0.ip", "11.10.0.152"),
					resource.TestCheckResourceAttrSet("vcd_vapp_vm."+netVmName1, "networks.0.mac"),

					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.1.orgnetwork", "multinic-net"),
					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.1.is_primary", "true"),
					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.1.ip_allocation_mode", "DHCP"),
					//resource.TestCheckResourceAttrSet("vcd_vapp_vm."+netVmName1, "networks.1.ip"), // We cannot guarantee DHCP
					resource.TestCheckResourceAttrSet("vcd_vapp_vm."+netVmName1, "networks.1.mac"),

					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.2.orgnetwork", "multinic-net"),
					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.2.is_primary", "false"),
					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.2.ip_allocation_mode", "MANUAL"),
					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.2.ip", "11.10.0.170"),
					resource.TestCheckResourceAttrSet("vcd_vapp_vm."+netVmName1, "networks.2.mac"),

					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.3.orgnetwork", "multinic-net2"),
					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.3.is_primary", "false"),
					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.3.ip_allocation_mode", "POOL"),
					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.3.ip", "12.10.0.152"),
					resource.TestCheckResourceAttrSet("vcd_vapp_vm."+netVmName1, "networks.3.mac"),

					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.4.orgnetwork", ""),
					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.4.is_primary", "false"),
					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.4.ip_allocation_mode", "NONE"),
					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.4.ip", ""),
					resource.TestCheckResourceAttrSet("vcd_vapp_vm."+netVmName1, "networks.4.mac"),

					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.5.orgnetwork", ""),
					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.5.is_primary", "false"),
					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.5.ip_allocation_mode", "NONE"),
					resource.TestCheckResourceAttr("vcd_vapp_vm."+netVmName1, "networks.5.ip", ""),
					resource.TestCheckResourceAttrSet("vcd_vapp_vm."+netVmName1, "networks.5.mac"),
				),
			},
		},
	})
}

////
const testAccCheckVcdVAppVmNetwork = `
	resource "vcd_network_routed" "net" {
	org = "{{.Org}}"
	vdc = "{{.Vdc}}"

	name         = "multinic-net"
	edge_gateway = "{{.EdgeGateway}}"
	gateway      = "11.10.0.1"

	dhcp_pool {
	  start_address = "11.10.0.2"
	  end_address   = "11.10.0.100"
	}

	static_ip_pool {
	  start_address = "11.10.0.152"
	  end_address   = "11.10.0.254"
	}
}

resource "vcd_network_routed" "net2" {
	org = "{{.Org}}"
	vdc = "{{.Vdc}}"

	name         = "multinic-net2"
	edge_gateway = "{{.EdgeGateway}}"
	gateway      = "12.10.0.1"

	static_ip_pool {
	  start_address = "12.10.0.152"
	  end_address   = "12.10.0.254"
	}
}

resource "vcd_vapp" "{{.VAppName}}" {
	org = "{{.Org}}"
	vdc = "{{.Vdc}}"

	name = "{{.VAppName}}"
}

resource "vcd_vapp_vm" "{{.VMName}}" {
	org = "{{.Org}}"
	vdc = "{{.Vdc}}"

	vapp_name     = "${vcd_vapp.{{.VAppName}}.name}"
	name          = "{{.VMName}}"
	catalog_name  = "{{.Catalog}}"
	template_name = "{{.CatalogItem}}"
	memory        = 512
	cpus          = 2
	cpu_cores     = 1

	networks = [{
	  orgnetwork = "${vcd_network_routed.net.name}"
	  ip_allocation_mode = "POOL"
	  is_primary         = false
		},
	  {
		orgnetwork = "${vcd_network_routed.net.name}"
		ip_allocation_mode = "DHCP"
		is_primary         = true
	  },
	  {
		orgnetwork         = "${vcd_network_routed.net.name}"
		ip                 = "11.10.0.170"
		ip_allocation_mode = "MANUAL"
		is_primary         = false
	  },
	  {
		orgnetwork         = "${vcd_network_routed.net2.name}"
		ip_allocation_mode = "POOL"
		is_primary         = false
	  },
	  {
		ip_allocation_mode = "NONE"
		ip = ""
		orgnetwork = ""
	  },
	  {
		ip_allocation_mode = "NONE"
	  },
	]
}
`