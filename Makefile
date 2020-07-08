.PHONY: clean all

BUILD_DIR = target
SRC_DIR = src

all: $(BUILD_DIR) $(BUILD_DIR)/main

include build/shared.mk

$(BUILD_DIR):
	@$(call run,mkdir -p $(BUILD_DIR),MKDIR $@)

$(BUILD_DIR)/main: $(SRC_DIR)/main.cc
	@$(call xrun,$(CXX) $$CXXFLAGS $$ARGS -o $@ $<,CXX $$ARGS $<)

clean:
	rm -rf $(BUILD_DIR)
