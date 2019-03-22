package vcd

import (
	"bytes"
	"fmt"
	"log"
	"sort"
	"strconv"

	"github.com/hashicorp/terraform/helper/hashcode"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/vmware/go-vcloud-director/v2/govcd"
	"github.com/vmware/go-vcloud-director/v2/types/v56"
)

func resourceVcdVAppVm() *schema.Resource {
	return &schema.Resource{
		Create: resourceVcdVAppVmCreate,
		Update: resourceVcdVAppVmUpdate,
		Read:   resourceVcdVAppVmRead,
		Delete: resourceVcdVAppVmDelete,

		Schema: map[string]*schema.Schema{
			"vapp_name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"org": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"vdc": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"template_name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			"catalog_name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			"memory": {
				Type:     schema.TypeInt,
				Optional: true,
			},
			"cpus": {
				Type:     schema.TypeInt,
				Optional: true,
			},
			"ip": {
				Type:             schema.TypeString,
				Optional:         true,
				Computed:         true,
				ConflictsWith:    []string{"networks"},
				Deprecated: "In favor of networks parameter",
				DiffSuppressFunc: suppressIfIpIsOneOf(),
			},
			"mac": {
				Type:     schema.TypeString,
				Optional: true,
				Computed: true,
			},
			"initscript": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"metadata": {
				Type:     schema.TypeMap,
				Optional: true,
				// For now underlying go-vcloud-director repo only supports
				// a value of type String in this map.
			},
			"href": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				Computed: true,
			},
			"accept_all_eulas": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  true,
			},
			"power_on": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  true,
			},
			"network_href": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"networks": {
				Type:          schema.TypeList,
				Optional:      true,
				ForceNew:      true,
				ConflictsWith: []string{"ip"},
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"orgnetwork": {
							Type:     schema.TypeString,
							Required: true,
						},
						"ip": {
							Type:             schema.TypeString,
							Optional:         true,
							Computed:         true,
							DiffSuppressFunc: suppressIfIpIsOneOf(),
						},
						"ip_allocation_mode": {
							Type:             schema.TypeString,
							Optional:         true,
							Default:          "POOL",
							ValidateFunc:     checkIpAddressAllocationMode(),
						},
						"is_primary": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"adapter_type": {
							Type:     schema.TypeString,
							Optional: true,
							ForceNew: true,
						},
						"mac": {
							Type:     schema.TypeString,
							Optional: true,
							Computed: true,
						},
					},
				},
			},
			"network_name": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
				Deprecated: "In favor of networks parameter",
			},
			"vapp_network_name": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"disk": {
				Type: schema.TypeSet,
				Elem: &schema.Resource{Schema: map[string]*schema.Schema{
					"name": {
						Type:     schema.TypeString,
						Required: true,
					},
					"bus_number": {
						Type:     schema.TypeString,
						Required: true,
					},
					"unit_number": {
						Type:     schema.TypeString,
						Required: true,
					},
				}},
				Optional: true,
				Set:      resourceVcdVmIndependentDiskHash,
			},
		},
	}
}

func checkIpAddressAllocationMode() schema.SchemaValidateFunc {
	return func(val interface{}, key string) (warns []string, errs []error) {
	 if v == "POOL" || v == "DHCP" || v == "MANUAL" || v == "NONE" {
		 return 0
	 }
	 errs = append(errs, fmt.Errorf("ip_address_allocation_mode must be one of POOL, DHCP, MANUAL or NONE got: %v", v))
   return 1
 }
}

func suppressIfIpIsOneOf() schema.SchemaDiffSuppressFunc {
	return func(k string, old string, new string, d *schema.ResourceData) bool {
		switch {
		case new == "dhcp" && old != "":
			return true
		case new == "allocated" && old != "":
			return true
		case new == "" && old != "":
			return true
		default:
			return false
		}
	}
}

func resourceVcdVAppVmCreate(d *schema.ResourceData, meta interface{}) error {
	vcdClient := meta.(*VCDClient)

	org, vdc, err := vcdClient.GetOrgAndVdcFromResource(d)
	if err != nil {
		return fmt.Errorf(errorRetrievingOrgAndVdc, err)
	}

	catalog, err := org.FindCatalog(d.Get("catalog_name").(string))
	if err != nil || catalog == (govcd.Catalog{}) {
		return fmt.Errorf("error finding catalog: %s", d.Get("catalog_name").(string))
	}

	catalogItem, err := catalog.FindCatalogItem(d.Get("template_name").(string))
	if err != nil || catalogItem == (govcd.CatalogItem{}) {
		return fmt.Errorf("error finding catalog item: %#v", err)
	}

	vappTemplate, err := catalogItem.GetVAppTemplate()
	if err != nil {
		return fmt.Errorf("error finding VAppTemplate: %#v", err)
	}

	acceptEulas := d.Get("accept_all_eulas").(bool)

	vapp, err := vdc.FindVAppByName(d.Get("vapp_name").(string))
	if err != nil {
		return fmt.Errorf("error finding vApp: %#v", err)
	}

	var network *types.OrgVDCNetwork

	if d.Get("network_name").(string) != "" {
		network, err = addVdcNetwork(d, vdc, vapp, vcdClient)
		if err != nil {
			return err
		}
	}

	vappNetworkName := d.Get("vapp_network_name").(string)
		if vappNetworkName != "" {
			isVappNetwork, err := isItVappNetwork(vappNetworkName, vapp)
			if err != nil {
				return err
			}
			if !isVappNetwork {
				return fmt.Errorf("vapp_network_name: %s is not found", vappNetworkName)
			}
		}
	}

	err = retryCall(vcdClient.MaxRetryTimeout, func() *resource.RetryError {
		log.Printf("[TRACE] Creating VM: %s", d.Get("name").(string))
		var networks []*types.OrgVDCNetwork
		if network != nil {
			networks = append(networks, network)
		}
		task, err := vapp.AddVM(networks, vappNetworkName, vappTemplate, d.Get("name").(string), acceptEulas)

		return fmt.Errorf("error finding Vapp: %#v", err)
	}

	netNames := []string{}
	var nets []map[string]interface{}
	network := d.Get("network_name").(string)
	networks := d.Get("networks").([]interface{})

	switch {
	// network_name is not set. networks is set in config
	case network == "" && len(networks) > 0:
		for _, network := range networks {
			n := network.(map[string]interface{})
			nets = append(nets, n)
			net, err := vdc.FindVDCNetwork(n["orgnetwork"].(string))
			if err != nil {
				return fmt.Errorf("Error finding OrgVCD Network: %#v", err)
			}
			netNames = append(netNames, net.OrgVDCNetwork.Name)
		}
		// network_name is set. networks is not set in config
	case network != "" && len(networks) == 0:
		network := map[string]interface{}{
			"ip":           d.Get("ip").(string),
			"is_primary":   true,
			"orgnetwork":   d.Get("network_name").(string),
			"adapter_type": "",
		}
		nets = append(nets, network)
		netNames = append(netNames, d.Get("network_name").(string))
	default:
		err = retryCall(vcdClient.MaxRetryTimeout, func() *resource.RetryError {
			task, err := vapp.AddRAWNetworkConfig()
			if err != nil {
				return resource.RetryableError(fmt.Errorf("error assigning network to vApp: %#v", err))
			}
			return resource.RetryableError(task.WaitTaskCompletion())
		})
	}

	vAppNetworkNames := []string{}
	vAppNetworkConfig, err := vapp.GetNetworkConfigSection()
	for _, vAppNetwork := range vAppNetworkConfig.NetworkConfig {
		vAppNetworkNames = append(vAppNetworkNames, vAppNetwork.NetworkName)
	}
	// this checkes if a network is assigned to a vapp
	if len(netNames) > 0 {
		m := make(map[string]bool)
		for i := 0; i < len(vAppNetworkNames); i++ {
			m[vAppNetworkNames[i]] = true
		}
		for _, netName := range netNames {
			// if the network is not assigned, assigne it to vapp
			if _, ok := m[netName]; !ok {
				err = retryCall(vcdClient.MaxRetryTimeout, func() *resource.RetryError {
					n, err := vdc.FindVDCNetwork(netName)
					task, err := vapp.AppendNetworkConfig(n.OrgVDCNetwork)
					if err != nil {
						return resource.RetryableError(fmt.Errorf("failed to add network to vapp: %#v", err))
					}
					return resource.RetryableError(task.WaitTaskCompletion())
				})
				if err != nil {
					return fmt.Errorf("the VDC networks '%s' must be assigned to the vApp. Currently this networks are assigned %s", netNames, vAppNetworkNames)
				}
			}
		}
	}

	log.Printf("[TRACE] Found networks attached to vApp: %s", netNames)
	log.Printf("[TRACE] Network Connections: %s", nets)

	err = retryCall(vcdClient.MaxRetryTimeout, func() *resource.RetryError {
		log.Printf("[TRACE] Creating VM: %s", d.Get("name").(string))
		task, err := vapp.AddVM(nets, vappTemplate, d.Get("name").(string), acceptEulas)
		if err != nil {
			return resource.RetryableError(fmt.Errorf("error adding VM: %#v", err))
		}
		return resource.RetryableError(task.WaitTaskCompletion())
	})

	if err != nil {
		return fmt.Errorf(errorCompletingTask, err)
	}

	vm, err := vdc.FindVMByName(vapp, d.Get("name").(string))

	if err != nil {
		d.SetId("")
		return fmt.Errorf("error getting VM1 : %#v", err)
	}

	if network != nil || vappNetworkName != "" {
		err = retryCall(vcdClient.MaxRetryTimeout, func() *resource.RetryError {
			var networksChanges []map[string]interface{}
			if vappNetworkName != "" {
				networksChanges = append(networksChanges, map[string]interface{}{
					"ip":         d.Get("ip").(string),
					"orgnetwork": vappNetworkName,
				})
			}
			if network != nil {
				networksChanges = append(networksChanges, map[string]interface{}{
					"ip":         d.Get("ip").(string),
					"orgnetwork": network.Name,
				})
			}

			task, err := vm.ChangeNetworkConfig(networksChanges, d.Get("ip").(string))
			if err != nil {
				return resource.RetryableError(fmt.Errorf("error with Networking change: %#v", err))
			}
			return resource.RetryableError(task.WaitTaskCompletion())
		})
	}
	if err != nil {
		return fmt.Errorf("error changing network: %#v", err)
	}

	initScript, ok := d.GetOk("initscript")

	if ok {
		err = retryCall(vcdClient.MaxRetryTimeout, func() *resource.RetryError {
			task, err := vm.RunCustomizationScript(d.Get("name").(string), initScript.(string))
			if err != nil {
				return resource.RetryableError(fmt.Errorf("error with setting init script: %#v", err))
			}
			return resource.RetryableError(task.WaitTaskCompletion())
		})
		if err != nil {
			return fmt.Errorf(errorCompletingTask, err)
		}
	}
	d.SetId(d.Get("name").(string))

	err = resourceVcdVAppVmUpdate(d, meta)
	if err != nil {
		errAttachedDisk := updateStateOfAttachedDisks(d, vm, vdc)
		if errAttachedDisk != nil {
			d.Set("disk", nil)
			return fmt.Errorf("error reading attached disks : %#v and internal error : %#v", errAttachedDisk, err)
		}
		return err
	}
	return nil
}

// Adds existing org VDC network to VM network configuration
// Returns configured OrgVDCNetwork for Vm, networkName, error if any occur
func addVdcNetwork(networkNameToAdd string, vdc govcd.Vdc, vapp govcd.VApp, vcdClient *VCDClient) (*types.OrgVDCNetwork, error) {
	if networkNameToAdd == "" {
		return &types.OrgVDCNetwork{}, fmt.Errorf("'network_name' must be valid when adding VM to raw vApp")
	}

	net, err := vdc.FindVDCNetwork(networkNameToAdd)
	if err != nil {
		return &types.OrgVDCNetwork{}, fmt.Errorf("network %s wasn't found as VDC network", networkNameToAdd)
	}
	vdcNetwork := net.OrgVDCNetwork

	vAppNetworkConfig, err := vapp.GetNetworkConfig()

	isAlreadyVappNetwork := false
	for _, networkConfig := range vAppNetworkConfig.NetworkConfig {
		if networkConfig.NetworkName == networkNameToAdd {
			log.Printf("[TRACE] VDC network found as vApp network: %s", networkNameToAdd)
			isAlreadyVappNetwork = true
		}
	}

	if !isAlreadyVappNetwork {
		err = retryCall(vcdClient.MaxRetryTimeout, func() *resource.RetryError {
			task, err := vapp.AppendNetworkConfig(vdcNetwork)
			if err != nil {
				return resource.RetryableError(fmt.Errorf("error assigning network to vApp: %#v", err))
			}
			return resource.RetryableError(task.WaitTaskCompletion())
		})

		if err != nil {
			return &types.OrgVDCNetwork{}, fmt.Errorf("error assigning network to vApp:: %#v", err)
		}
	}

	return vdcNetwork, nil
}

// Checks if vapp network available for using
func isItVappNetwork(vAppNetworkName string, vapp govcd.VApp) (bool, error) {
	vAppNetworkConfig, err := vapp.GetNetworkConfig()
	if err != nil {
		return false, fmt.Errorf("error getting vApp networks: %#v", err)
	}

	for _, networkConfig := range vAppNetworkConfig.NetworkConfig {
		if networkConfig.NetworkName == vAppNetworkName {
			log.Printf("[TRACE] vApp network found: %s", vAppNetworkName)
			return true, nil
		}
	}

	return false, fmt.Errorf("configured vApp network isn't found: %#v", err)
}

type diskParams struct {
	name       string
	busNumber  *int
	unitNumber *int
}

func expandDisksProperties(v interface{}) ([]diskParams, error) {
	v = v.(*schema.Set).List()
	l := v.([]interface{})
	diskParamsArray := make([]diskParams, 0, len(l))

	for _, raw := range l {
		if raw == nil {
			continue
		}
		original := raw.(map[string]interface{})
		addParams := diskParams{name: original["name"].(string)}

		busNumber := original["bus_number"].(string)
		if busNumber != "" {
			convertedBusNumber, err := strconv.Atoi(busNumber)
			if err != nil {
				return nil, fmt.Errorf("value `%s` bus_number is not number. err: %#v", busNumber, err)
			}
			addParams.busNumber = &convertedBusNumber
		}

		unitNumber := original["unit_number"].(string)
		if unitNumber != "" {
			convertedUnitNumber, err := strconv.Atoi(unitNumber)
			if err != nil {
				return nil, fmt.Errorf("value `%s` unit_number is not number. err: %#v", unitNumber, err)
			}
			addParams.unitNumber = &convertedUnitNumber
		}

		diskParamsArray = append(diskParamsArray, addParams)
	}
	return diskParamsArray, nil
}

func getVmIndependentDisks(vm govcd.VM) []string {

	var disks []string
	for _, item := range vm.VM.VirtualHardwareSection.Item {
		// disk resource type is 17
		if item.ResourceType == 17 && "" != item.HostResource[0].Disk {
			disks = append(disks, item.HostResource[0].Disk)
		}
	}
	return disks
}

func resourceVcdVAppVmUpdate(d *schema.ResourceData, meta interface{}) error {

	vcdClient := meta.(*VCDClient)

	_, vdc, err := vcdClient.GetOrgAndVdcFromResource(d)
	if err != nil {
		return fmt.Errorf(errorRetrievingOrgAndVdc, err)
	}

	vapp, err := vdc.FindVAppByName(d.Get("vapp_name").(string))

	if err != nil {
		return fmt.Errorf("error finding vApp: %s", err)
	}

	vm, err := vdc.FindVMByName(vapp, d.Get("name").(string))

	if err != nil {
		d.SetId("")
		return fmt.Errorf("error getting VM2: %#v", err)
	}

	status, err := vm.GetStatus()
	if err != nil {
		return fmt.Errorf("error getting VM status: %#v", err)
	}

	if d.HasChange("metadata") {
		oldRaw, newRaw := d.GetChange("metadata")
		oldMetadata := oldRaw.(map[string]interface{})
		newMetdata := newRaw.(map[string]interface{})
		var toBeRemovedMetadata []string
		// Check if any key in old metadata was removed in new metadata.
		// Creates a list of keys to be removed.
		for k := range oldMetadata {
			if _, ok := newMetdata[k]; !ok {
				toBeRemovedMetadata = append(toBeRemovedMetadata, k)
			}
		}
		for _, k := range toBeRemovedMetadata {
			task, err := vm.DeleteMetadata(k)
			if err != nil {
				return fmt.Errorf("error deleting metadata: %#v", err)
			}
			err = task.WaitTaskCompletion()
			if err != nil {
				return fmt.Errorf(errorCompletingTask, err)
			}
		}
		for k, v := range newMetdata {
			task, err := vm.AddMetadata(k, v.(string))
			if err != nil {
				return fmt.Errorf("error adding metadata: %#v", err)
			}
			err = task.WaitTaskCompletion()
			if err != nil {
				return fmt.Errorf(errorCompletingTask, err)
			}
		}
	}

	if d.HasChange("memory") || d.HasChange("cpus") || d.HasChange("networks") || d.HasChange("power_on") {
		if status != "POWERED_OFF" {
			task, err := vm.PowerOff()
			if err != nil {
				return fmt.Errorf("error Powering Off: %#v", err)
			}
			err = task.WaitTaskCompletion()
			if err != nil {
				return fmt.Errorf(errorCompletingTask, err)
			}
		}

		// detaching independent disks - only possible when VM power off
		if d.HasChange("disk") {
			err = attachDetachDisks(d, vm, vdc)
			if err != nil {
				errAttachedDisk := updateStateOfAttachedDisks(d, vm, vdc)
				if errAttachedDisk != nil {
					d.Set("disk", nil)
					return fmt.Errorf("error reading attached disks : %#v and internal error : %#v", errAttachedDisk, err)
				}
				return fmt.Errorf("error attaching-detaching  disks when updating resource : %#v", err)
			}
		}
		if d.HasChange("memory") {
			err = retryCall(vcdClient.MaxRetryTimeout, func() *resource.RetryError {
				task, err := vm.ChangeMemorySize(d.Get("memory").(int))
				if err != nil {
					return resource.RetryableError(fmt.Errorf("error changing memory size: %#v", err))
				}

				return resource.RetryableError(task.WaitTaskCompletion())
			})
			if err != nil {
				return err
			}
		}

		if d.HasChange("cpus") {
			err = retryCall(vcdClient.MaxRetryTimeout, func() *resource.RetryError {
				task, err := vm.ChangeCPUCount(d.Get("cpus").(int))
				if err != nil {
					return resource.RetryableError(fmt.Errorf("error changing cpu count: %#v", err))
				}

				return resource.RetryableError(task.WaitTaskCompletion())
			})
			if err != nil {
				return fmt.Errorf(errorCompletingTask, err)
			}
		}

		if d.HasChange("networks") {
			n := []map[string]interface{}{}

			nets := d.Get("networks").([]interface{})
			for _, network := range nets {
				n = append(n, network.(map[string]interface{}))
			}
			err = retryCall(vcdClient.MaxRetryTimeout, func() *resource.RetryError {
				task, err := vm.ChangeNetworkConfig(n, d.Get("ip").(string))
				if err != nil {
					return resource.RetryableError(fmt.Errorf("error changing network: %#v", err))
				}
				return resource.RetryableError(task.WaitTaskCompletion())
			})
			if err != nil {
				return fmt.Errorf(errorCompletingTask, err)
			}
		}

		if d.Get("power_on").(bool) {
			err = retryCall(vcdClient.MaxRetryTimeout, func() *resource.RetryError {
				task, err := vm.PowerOn()
				if err != nil {
					return resource.RetryableError(fmt.Errorf("error Powering Up: %#v", err))
				}

				return resource.RetryableError(task.WaitTaskCompletion())
			})
			if err != nil {
				return fmt.Errorf(errorCompletingTask, err)
			}
		}

	}

	return resourceVcdVAppVmRead(d, meta)
}

// updates attached disks to latest state. Removed not needed and add new ones
func attachDetachDisks(d *schema.ResourceData, vm govcd.VM, vdc govcd.Vdc) error {
	oldValues, newValues := d.GetChange("disk")

	attachDisks := newValues.(*schema.Set).Difference(oldValues.(*schema.Set))
	detachDisks := oldValues.(*schema.Set).Difference(newValues.(*schema.Set))

	removeDiskProperties, err := expandDisksProperties(detachDisks)
	if err != nil {
		return err
	}

	for _, diskData := range removeDiskProperties {
		disk, err := vdc.QueryDisk(diskData.name)
		if err != nil {
			return fmt.Errorf("did not find disk `%s`: %#v", diskData.name, err)
		}

		attachParams := &types.DiskAttachOrDetachParams{Disk: &types.Reference{HREF: disk.Disk.HREF}}
		if diskData.unitNumber != nil {
			attachParams.UnitNumber = diskData.unitNumber
		}
		if diskData.busNumber != nil {
			attachParams.BusNumber = diskData.busNumber
		}

		task, err := vm.DetachDisk(attachParams)
		if err != nil {
			return fmt.Errorf("error detaching disk `%s` to vm %#v", diskData.name, err)
		}
		err = task.WaitTaskCompletion()
		if err != nil {
			return fmt.Errorf("error waiting for task to complete detaching disk `%s` to vm %#v", diskData.name, err)
		}
	}

	// attach new independent disks
	newDiskProperties, err := expandDisksProperties(attachDisks)
	if err != nil {
		return err
	}

	sort.SliceStable(newDiskProperties, func(i, j int) bool {
		if newDiskProperties[i].busNumber == newDiskProperties[j].busNumber {
			return *newDiskProperties[i].unitNumber > *newDiskProperties[j].unitNumber
		}
		return *newDiskProperties[i].busNumber > *newDiskProperties[j].busNumber
	})

	for _, diskData := range newDiskProperties {
		disk, err := vdc.QueryDisk(diskData.name)
		if err != nil {
			return fmt.Errorf("did not find disk `%s`: %#v", diskData.name, err)
		}

		attachParams := &types.DiskAttachOrDetachParams{Disk: &types.Reference{HREF: disk.Disk.HREF}}
		if diskData.unitNumber != nil {
			attachParams.UnitNumber = diskData.unitNumber
		}
		if diskData.busNumber != nil {
			attachParams.BusNumber = diskData.busNumber
		}

		task, err := vm.AttachDisk(attachParams)
		if err != nil {
			return fmt.Errorf("error attaching disk `%s` to vm %#v", diskData.name, err)
		}
		err = task.WaitTaskCompletion()
		if err != nil {
			return fmt.Errorf("error waiting for task to complete attaching disk `%s` to vm %#v", diskData.name, err)
		}
	}
	return nil
}

func resourceVcdVAppVmRead(d *schema.ResourceData, meta interface{}) error {
	vcdClient := meta.(*VCDClient)

	_, vdc, err := vcdClient.GetOrgAndVdcFromResource(d)
	if err != nil {
		return fmt.Errorf(errorRetrievingOrgAndVdc, err)
	}

	vapp, err := vdc.FindVAppByName(d.Get("vapp_name").(string))

	if err != nil {
		return fmt.Errorf("error finding vApp: %s", err)
	}

	vm, err := vdc.FindVMByName(vapp, d.Get("name").(string))

	if err != nil {
		d.SetId("")
		return fmt.Errorf("error getting VM3 : %#v", err)
	}

	d.Set("name", vm.VM.Name)
	networks := d.Get("networks").([]interface{})
	network := d.Get("network_name").(string)
	switch {
	// network_name is not set. networks is set in config
	case network != "" && len(networks) == 0:
		d.Set("ip", vm.VM.NetworkConnectionSection.NetworkConnection[0].IPAddress)
		d.Set("mac", vm.VM.NetworkConnectionSection.NetworkConnection[0].MACAddress)
	case network == "" && len(networks) > 0:
		var networks []map[string]interface{}
		for index, net := range d.Get("networks").([]interface{}) {
			n := net.(map[string]interface{})
			n["ip"] = vm.VM.NetworkConnectionSection.NetworkConnection[index].IPAddress
			n["mac"] = vm.VM.NetworkConnectionSection.NetworkConnection[index].MACAddress
			networks = append(networks, n)
			d.Set("networks", networks)
		}
	}
	d.Set("href", vm.VM.HREF)

	err = updateStateOfAttachedDisks(d, vm, vdc)
	if err != nil {
		d.Set("disk", nil)
		return fmt.Errorf("error reading attached disks : %#v", err)
	}

	return nil
}

func updateStateOfAttachedDisks(d *schema.ResourceData, vm govcd.VM, vdc govcd.Vdc) error {
	// Check VM independent disks state
	diskProperties, err := expandDisksProperties(d.Get("disk"))
	if err != nil {
		return err
	}

	existingDisks := getVmIndependentDisks(vm)
	transformed := schema.NewSet(resourceVcdVmIndependentDiskHash, []interface{}{})

	for _, existingDiskHref := range existingDisks {
		disk, err := vdc.FindDiskByHREF(existingDiskHref)
		if err != nil {
			return fmt.Errorf("did not find disk `%s`: %#v", existingDiskHref, err)
		}

		// where isn't way to find bus_number and unit_number, so need copied from old values to not lose them
		var oldValues diskParams
		for _, oldDiskData := range diskProperties {
			if oldDiskData.name == disk.Disk.Name {
				oldValues = diskParams{name: oldDiskData.name, busNumber: oldDiskData.busNumber, unitNumber: oldDiskData.unitNumber}
			}
		}

		newValues := map[string]interface{}{
			"name": disk.Disk.Name,
		}

		if (diskParams{}) != oldValues {
			if nil != oldValues.busNumber {
				newValues["bus_number"] = strconv.Itoa(*oldValues.busNumber)
			}
			if nil != oldValues.unitNumber {
				newValues["unit_number"] = strconv.Itoa(*oldValues.unitNumber)
			}
		}

		transformed.Add(newValues)
	}

	d.Set("disk", transformed)
	return nil
}

func resourceVcdVAppVmDelete(d *schema.ResourceData, meta interface{}) error {
	vcdClient := meta.(*VCDClient)

	_, vdc, err := vcdClient.GetOrgAndVdcFromResource(d)
	if err != nil {
		return fmt.Errorf(errorRetrievingOrgAndVdc, err)
	}

	vapp, err := vdc.FindVAppByName(d.Get("vapp_name").(string))

	if err != nil {
		return fmt.Errorf("error finding vApp: %s", err)
	}

	vm, err := vdc.FindVMByName(vapp, d.Get("name").(string))

	if err != nil {
		return fmt.Errorf("error getting VM4 : %#v", err)
	}

	status, err := vm.GetStatus()
	if err != nil {
		return fmt.Errorf("error getting VM status: %#v", err)
	}

	log.Printf("[TRACE] VM Status: %s", status)
	if status != "POWERED_OFF" {
		log.Printf("[TRACE] Undeploying vApp: %s", vapp.VApp.Name)
		err = retryCall(vcdClient.MaxRetryTimeout, func() *resource.RetryError {
			task, err := vapp.Undeploy()
			if err != nil {
				return resource.RetryableError(fmt.Errorf("error Undeploying: %#v", err))
			}

			return resource.RetryableError(task.WaitTaskCompletion())
		})
		if err != nil {
			return fmt.Errorf("error Undeploying vApp: %#v", err)
		}
	}

	err = retryCall(vcdClient.MaxRetryTimeout, func() *resource.RetryError {
		log.Printf("[TRACE] Removing VM: %s", vm.VM.Name)
		err := vapp.RemoveVM(vm)
		if err != nil {
			return resource.RetryableError(fmt.Errorf("error deleting: %#v", err))
		}

		return nil
	})

	if status != "POWERED_OFF" {
		log.Printf("[TRACE] Redeploying vApp: %s", vapp.VApp.Name)
		task, err := vapp.Deploy()
		if err != nil {
			return fmt.Errorf("error Deploying vApp: %#v", err)
		}
		err = task.WaitTaskCompletion()
		if err != nil {
			return fmt.Errorf(errorCompletingTask, err)
		}

		log.Printf("[TRACE] Powering on vApp: %s", vapp.VApp.Name)
		err = retryCall(vcdClient.MaxRetryTimeout, func() *resource.RetryError {
			task, err = vapp.PowerOn()
			if err != nil {
				return resource.RetryableError(fmt.Errorf("error Powering Up vApp: %#v", err))
			}

			return resource.RetryableError(task.WaitTaskCompletion())
		})
		if err != nil {
			return fmt.Errorf("error Powering Up vApp: %#v", err)
		}
	}

	return err
}

func resourceVcdVmIndependentDiskHash(v interface{}) int {
	var buf bytes.Buffer
	m := v.(map[string]interface{})
	buf.WriteString(fmt.Sprintf("%s-",
		m["name"].(string)))
	if nil != m["bus_number"] {
		buf.WriteString(fmt.Sprintf("%s-",
			m["bus_number"].(string)))
	}
	if nil != m["unit_number"] {
		buf.WriteString(fmt.Sprintf("%s-",
			m["unit_number"].(string)))
	}
	return hashcode.String(buf.String())
}
