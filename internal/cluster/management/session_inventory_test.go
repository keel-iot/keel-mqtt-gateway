package management

import "testing"

func TestParseInventoryPageDefaultsAndBounds(t *testing.T) {
	page, size, err := parseInventoryPage("", "")
	if err != nil || page != 1 || size != 50 {
		t.Fatalf("defaults = %d/%d, err=%v", page, size, err)
	}
	if _, _, err := parseInventoryPage("0", "50"); err == nil {
		t.Fatal("expected page lower bound error")
	}
	if _, _, err := parseInventoryPage("1", "101"); err == nil {
		t.Fatal("expected page size upper bound error")
	}
}

func TestInventoryMatchesDeviceFieldsAndSubscriptions(t *testing.T) {
	item := sessionInventoryItem{
		ClientID:      "device-42",
		Username:      "tenant-a",
		RemoteAddr:    "10.0.0.4:1883",
		Subscriptions: []string{"commands/device-42"},
	}
	for _, search := range []string{"DEVICE-42", "TENANT-A", "10.0.0.4", "commands/device-42"} {
		if !inventoryMatches(item, search) {
			t.Errorf("search %q did not match item", search)
		}
	}
	if inventoryMatches(item, "device-99") {
		t.Fatal("unexpected match for another device")
	}
}
