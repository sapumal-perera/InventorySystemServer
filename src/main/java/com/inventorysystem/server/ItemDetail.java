package com.inventorysystem.server;


public class ItemDetail {
   private String itemCode = "1";

    public ItemDetail(String itemCode, String name, int quantity, double price) {
        this.itemCode = itemCode;
        this.name = name;
        this.quantity = quantity;
        this.price = price;
    }

    private  String name = "2";
    private  int quantity = 3;
    private  double price = 4;

    public String getItemCode() {
        return itemCode;
    }

    public void setItemCode(String itemCode) {
        this.itemCode = itemCode;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
